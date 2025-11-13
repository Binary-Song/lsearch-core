pub mod core;
mod prelude;

use crate::core::Error;
use core::index_directory;
use core::IndexArgs;
use num_cpus;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitCode;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::Stdout;

#[derive(Deserialize)]
#[serde(tag = "request_type", content = "request_data")]
enum Request {
    IndexDirectory(IndexRequest),
}

#[derive(Serialize)]
#[serde(tag = "response_type", content = "response_data")]
enum Response {
    IndexResponse(IndexResponse),
    Error { message: String },
}

#[derive(Deserialize)]
struct IndexRequest {
    target_dir: String,
    output_dir: String,
    includes: Vec<String>,
    excludes: Vec<String>,
    read_chunk_size: usize,
    channel_capacity: usize,
    break_size: usize,
    workers: usize,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
enum IndexResponse {
    GlobUpdated {
        entries: usize,
    },
    GlobDone,
    IndexAdded {
        finished_entries: usize,
        total_entries: usize,
    },
    IndexWritten {
        path: String,
    },
    Finished,
}

async fn handle_index_directory_request(
    args: IndexRequest,
    mut out: Pin<&mut impl tokio::io::AsyncWrite>,
) -> Result<(), Error> {
    let args = IndexArgs {
        target_dir: args.target_dir,
        includes: args.includes,
        excludes: args.excludes,
        read_chunk_size: args.read_chunk_size,
        channel_capacity: args.channel_capacity,
        output_dir: args.output_dir,
        workers: args.workers,
        break_size: args.break_size,
    };
    let (send, mut recv) = tokio::sync::mpsc::channel(args.channel_capacity);

    // Spawn index_directory as a task so it runs concurrently with the message receiving loop
    let index_task =  tokio::spawn(index_directory(args.clone(), send.clone()));

    while let Some(event) = recv.recv().await {
        let resp = match event {
            core::Progress::GlobUpdated { entries } => IndexResponse::GlobUpdated { entries },
            core::Progress::GlobDone => IndexResponse::GlobDone,
            core::Progress::IndexAdded {
                finished_entries,
                total_entries,
            } => IndexResponse::IndexAdded {
                finished_entries,
                total_entries,
            },
            core::Progress::IndexWritten { output_path } => IndexResponse::IndexWritten {
                path: output_path.to_string_lossy().to_string(),
            },
            core::Progress::FileSearched { .. } => {
                // This is for search progress, not index progress - skip it
                continue;
            }
        };
        let v = serde_json::to_value(resp).map_err(|e| Error::JsonError {
            reason: "s1",
            inner: e,
        })?;
        let line = serde_json::to_string(&v).map_err(|e| Error::JsonError {
            reason: "s2",
            inner: e,
        })? + "\n";
        out.as_mut()
            .write_all(line.as_bytes())
            .await
            .map_err(|e| Error::CannotWrite { inner_err: e })?;
    }

    // Wait for the indexing task to complete and return its result
    index_task.await;
    Ok(())
}

async fn handle_msg(msg: Value, out: Pin<&mut impl tokio::io::AsyncWrite>) -> Result<(), Error> {
    let msg: Request = serde_json::from_value(msg).map_err(|e| Error::JsonError {
        reason: "s3",
        inner: e,
    })?;
    match msg {
        Request::IndexDirectory(args) => handle_index_directory_request(args, out).await,
    }
}

async fn main_loop(
    mut input: Pin<&mut impl tokio::io::AsyncBufRead>,
    mut out: Pin<&mut impl tokio::io::AsyncWrite>,
) -> Result<(), Error> {
    let mut lines = input.as_mut().lines();
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| Error::CannotRead { inner_err: e })?
    {
        let serde_value = match serde_json::from_str::<Value>(&line) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::JsonError {
                    reason: "s4",
                    inner: e,
                });
            }
        };
        handle_msg(serde_value, out.as_mut()).await?;
    }
    return Ok(());
}

#[tokio::main]
async fn main() -> ExitCode {
    let stdin = tokio::io::stdin();
    let stdin = BufReader::new(stdin);
    let stdout = tokio::io::stdout();
    let mut stdin = Box::pin(stdin);
    let mut stdout = Box::pin(stdout);
    let result = main_loop(stdin.as_mut(), stdout.as_mut()).await;
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            let s = serde_json::to_string(&Response::Error {
                message: format!("{}", e),
            })
            .unwrap();
            let b = s.as_bytes();
            stdout.as_mut().write_all(b).await.ok();
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod test {
    use crate::*;

    #[tokio::test]
    async fn index_dir_test() {
        let dir = "E:\\trash\\chromium-main\\";
        println!("{:?}", dir);
        let test_res_dir = PathBuf::from(dir);
        let output_index_path = test_res_dir.join(".index");
        let result = handle_index_directory_request(
            IndexRequest {
                target_dir: test_res_dir.to_str().unwrap().to_string(),
                includes: vec!["*".to_string()],
                excludes: vec!["*.ignoreme".to_string(), "*.index".to_string()],
                read_chunk_size: 1024 * 512,
                channel_capacity: 16,
                break_size: 1024 * 1024 * 64,
                output_dir: output_index_path.to_str().unwrap().to_string(),
                workers: num_cpus::get(),
            },
            Pin::new(&mut tokio::io::stdout()),
        )
        .await;
        if let Err(e) = result {
            panic!("Test failed with error: {:?}", e);
        }
    }
}
