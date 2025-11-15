use crate::core::index_directory;
use crate::core::Error;
use crate::core::IndexArgs;
use crate::core::Progress;
use crate::dbg_loc;
use crate::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;

#[derive(Deserialize)]
pub struct IndexRequest {
    pub target_dir: String,
    pub output_dir: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub read_chunk_size: usize,
    pub channel_capacity: usize,
    pub break_size: usize,
    pub workers: usize,
    pub use_glob_cache: bool,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
pub enum IndexResponse {
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
    ErrorOccurred {
        message: String,
    },
    Done,
}

pub async fn handle_index_directory_request(
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
        use_glob_cache: args.use_glob_cache,
    };
    let (send, mut recv) = tokio::sync::mpsc::channel(args.channel_capacity);
    // Spawn index_directory as a task so it runs concurrently with the message receiving loop
    let index_task = tokio::spawn(index_directory(args.clone(), send));
    while let Some(event) = recv.recv().await {
        let resp = match event {
            Progress::GlobUpdated { entries } => IndexResponse::GlobUpdated { entries },
            Progress::GlobDone => IndexResponse::GlobDone,
            Progress::IndexAdded {
                finished_entries,
                total_entries,
            } => IndexResponse::IndexAdded {
                finished_entries,
                total_entries,
            },
            Progress::IndexWritten { output_path } => IndexResponse::IndexWritten {
                path: output_path.to_string_lossy().to_string(),
            },
            Progress::FileSearched { .. } => {
                // This is for search progress, not index progress - skip it
                continue;
            }
            Progress::ErrorOccurred { message } => IndexResponse::ErrorOccurred { message },
            Progress::Done => IndexResponse::Done,
        };
        let v = serde_json::to_value(resp).map_error(dbg_loc!())?;
        let line = serde_json::to_string(&v).map_error(dbg_loc!())? + "\n";
        out.as_mut()
            .write_all(line.as_bytes())
            .await
            .map_error(dbg_loc!())?;
    }

    // Wait for the indexing task to complete and return its result
    index_task
        .await
        .map_error(dbg_loc!())?
        .map_error(dbg_loc!())?;
    Ok(())
}
