mod index_request;
mod search_request;
use crate::core::Error;
use crate::dbg_loc;
use crate::prelude::IntoError;
use crate::prelude::MapError;
use crate::CommandLineArgs;
use crate::Mode;
use index_request::*;
use num_cpus;
use search_request::*;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitCode;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;

#[derive(Deserialize)]
#[serde(tag = "request_type", content = "request_data")]
enum Request {
    IndexDirectory(IndexRequest),
    Search(SearchRequest),
}

#[derive(Serialize)]
#[serde(tag = "response_type", content = "response_data")]
enum Response {
    Listening { host: String, port: u16 },
    IndexResponse(IndexResponse),
    SearchResponse(SearchResponse),
    Error { message: String },
}

async fn handle_msg(msg: Value, out: Pin<&mut impl tokio::io::AsyncWrite>) -> Result<(), Error> {
    let msg: Request = serde_json::from_value(msg).map_error(dbg_loc!())?;
    match msg {
        Request::IndexDirectory(args) => handle_index_directory_request(args, out).await,
        Request::Search(args) => todo!(),
    }
}

async fn main_loop(
    mut input: Pin<&mut impl tokio::io::AsyncBufRead>,
    mut out: Pin<&mut impl tokio::io::AsyncWrite>,
) -> Result<(), Error> {
    let mut lines = input.as_mut().lines();
    while let Some(line) = lines.next_line().await.map_error(dbg_loc!())? {
        let serde_value = match serde_json::from_str::<Value>(&line) {
            Ok(v) => v,
            Err(e) => {
                return Err(e.into_error(dbg_loc!()));
            }
        };
        handle_msg(serde_value, out.as_mut()).await?;
    }
    return Ok(());
}
pub async fn start_tcp_server(args: &CommandLineArgs) -> Result<(), Error> {
    match &args.mode {
        Mode::Json { host, port } => {
            // Start TCP Server and serve 1 connection
            let listener = TcpListener::bind(format!("{}:{}", host, port))
                .await
                .map_error(dbg_loc!())?;
            let local_addr = listener.local_addr().map_error(dbg_loc!())?;
            let resp = Response::Listening {
                host: local_addr.ip().to_string(),
                port: local_addr.port(),
            };
            println!("{}", serde_json::to_string(&resp).map_error(dbg_loc!())?);
            let (stream, _addr) = listener.accept().await.map_error(dbg_loc!())?;
            let (read_half, mut write_half) = io::split(stream);
            let mut reader = BufReader::new(read_half);
            let reader_pin = Pin::new(&mut reader);
            let writer_pin = Pin::new(&mut write_half);
            main_loop(reader_pin, writer_pin).await?;
            Ok(())
        }
        _ => Err("Mode should be Json.".into_error(dbg_loc!())),
    }
}

pub async fn entry_point(args: &CommandLineArgs) -> bool {
    start_tcp_server(args).await.is_ok()
}
