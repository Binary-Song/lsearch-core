use crate::core::index_directory;
use crate::core::search::{search_in_index_files, SearchArgs};
use crate::core::Error;
use crate::core::IndexArgs;
use crate::dbg_loc;
use crate::prelude::IntoError;
use crate::prelude::MapError;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;

#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SearchResponse {
    FileFound {
        file: String,
    },
    IndexFileSearched {
        finished_files: usize,
        total_files: usize,
    },
    ErrorOccurred {
        message: String,
    },
    Done,
}
#[derive(Deserialize, Default)]
pub struct SearchRequest {
    target_dir: String,
    substring: String,
    workers: usize,
}

pub async fn handle_search_request(
    args: SearchRequest,
    mut out: Pin<&mut impl tokio::io::AsyncWrite>,
) -> Result<(), Error> {
    // The target_dir is now the index directory (not the directory being searched)
    let index_dir = PathBuf::from(&args.target_dir);
    if !index_dir.exists() {
        return Err(format!("Index dir {} does not exist", args.target_dir).into_error(dbg_loc!()));
    }

    if !index_dir.is_dir() {
        return Err(format!("Target {} is not a directory", args.target_dir).into_error(dbg_loc!()));
    }

    let search_args = SearchArgs {
        index_dir,
        workers: args.workers,
    };

    let (sender, mut recvr) = tokio::sync::mpsc::channel(100);
    let query = args.substring.as_bytes().to_vec();

    // Spawn search task
    let search_task =
        tokio::spawn(async move { search_in_index_files(search_args, query, sender).await });

    // Stream progress events
    while let Some(event) = recvr.recv().await {
        let resp = match event {
            crate::core::Progress::FileSearched {
                finished_files,
                total_files,
            } => SearchResponse::IndexFileSearched {
                finished_files,
                total_files,
            },
            crate::core::Progress::ErrorOccurred { message } => {
                SearchResponse::ErrorOccurred { message }
            }
            _ => continue,
        };
        let v = serde_json::to_value(resp).map_error(dbg_loc!())?;
        let line = serde_json::to_string(&v).map_error(dbg_loc!())? + "\n";
        out.as_mut()
            .write_all(line.as_bytes())
            .await
            .map_error(dbg_loc!())?;
    }

    // Get results and output file names
    let results = search_task
        .await
        .map_error(dbg_loc!())?
        .map_error(dbg_loc!())?;

    for result in results {
        let resp = SearchResponse::FileFound {
            file: result.file_path.to_string_lossy().to_string(),
        };
        let v = serde_json::to_value(resp).map_error(dbg_loc!())?;
        let line = serde_json::to_string(&v).map_error(dbg_loc!())? + "\n";
        out.as_mut()
            .write_all(line.as_bytes())
            .await
            .map_error(dbg_loc!())?;
    }

    // Send Done message
    let resp = SearchResponse::Done;
    let v = serde_json::to_value(resp).map_error(dbg_loc!())?;
    let line = serde_json::to_string(&v).map_error(dbg_loc!())? + "\n";
    out.as_mut()
        .write_all(line.as_bytes())
        .await
        .map_error(dbg_loc!())?;

    Ok(())
}
