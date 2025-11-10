use crate::core::error::Error;
use async_stream::stream;
use futures::future::select;
use futures::future::Either;
use futures::pin_mut;
use futures::stream::Select;
use std::future::Future;
use std::marker::Send;
use std::pin::Pin;
use std::process::Output;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::AbortHandle;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub enum TaskUpdate<TFinal, TYield> {
    Yield(TYield),
    Final(TFinal),
    Error(Error),
}

#[derive(Debug)]
pub struct Channel<S> {
    sender: tokio::sync::mpsc::Sender<S>,
}

impl<T> Channel<T> {
    pub async fn send(&self, progress: T) -> Result<(), Error> {
        self.sender
            .send(progress)
            .await
            .map_err(|e| Error::SendError {
                message: format!("Failed to send: {}", e),
            })
    }
}

struct Task<S, TFinal, TYield>
where
    S: Stream<Item = TaskUpdate<TFinal, TYield>>,
{
    stream: Pin<Box<S>>,
    abort_handle: AbortHandle,
}

impl<S, TFinal, TYield> Drop for Task<S, TFinal, TYield>
where
    S: Stream<Item = TaskUpdate<TFinal, TYield>>,
{
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

impl<S, TFinal, TYield> Stream for Task<S, TFinal, TYield>
where
    S: Stream<Item = TaskUpdate<TFinal, TYield>>,
{
    type Item = TaskUpdate<TFinal, TYield>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// Spawns a task that can report progress via yields.
///
/// - `task_generator`: A function/closure that takes a `Yielder` and, returns an async block (or anything that implements the `Future` trait, we will just call it 'the async block' from now on).
///
///   *Yield* means producing intermediate results. You *yield* by calling `Yielder::yield_with` within the async block. The intermediate results has type `TYield`.
///
///    The async block returns a final result of type `TFinal`.
///
/// - return value: A tuple of:
///   - An async stream of `TaskUpdate<TFinal, TYield>` that yields progress updates and finally the result or error. Implements `tokio_stream::Stream`.
///   - An `AbortHandle` that can be used to abort the task.
pub fn spawn_task<TFinal, TYield, TFuture>(
    task_generator: impl FnOnce(Channel<TYield>) -> TFuture,
    channel_buffer: usize,
) -> impl Stream<Item = TaskUpdate<TFinal, TYield>> + Drop
where
    TFinal: Send + Unpin + 'static,
    TYield: Send + Unpin + 'static,
    TFuture: Future<Output = TFinal> + Send + 'static,
{
    let (sender, mut recvr) = tokio::sync::mpsc::channel::<TYield>(channel_buffer);
    let yielder = Channel { sender };
    let future = task_generator(yielder);
    let join_fut = tokio::spawn(future);
    let abort_handle = join_fut.abort_handle();
    let strm = stream! {
        let mut join_fut = Box::pin(join_fut);
        loop { // keep recving
            let recv_fut = Box::pin(recvr.recv());
            match select(join_fut, recv_fut).await {
                // the task is finished
                Either::Left((join_res, _)) => {
                    match join_res {
                        Ok(t) => {
                            yield TaskUpdate::Final(t);
                            break;
                        },
                        Err(e) => {
                            yield TaskUpdate::Error(Error::TaskDiedWithJoinError { inner: e });
                            break;
                        }
                    };
                }
                // the task yields (aka we recved something), continue recving
                Either::Right((recv_res, join_fut2)) => {
                   match recv_res {
                        Some(r) => {
                            yield TaskUpdate::Yield(r);
                            join_fut = join_fut2;
                            continue;
                        }
                        None => {
                            yield TaskUpdate::Error(Error::TaskClosedTheChannel);
                            break;
                        },
                    };
                }
            }
        };
    };
    Task {
        stream: Box::pin(strm),
        abort_handle,
    }
}
