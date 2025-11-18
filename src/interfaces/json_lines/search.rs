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
    // Find all .index files in target_dir
    let target_path = PathBuf::from(&args.target_dir);
    if !target_path.exists() {
        return Err(format!("Target dir {} does not exist", args.target_dir).into_error(dbg_loc!()));
    }

    let mut index_files = Vec::new();
    if target_path.is_dir() {
        let mut entries = tokio::fs::read_dir(&target_path)
            .await
            .map_error(dbg_loc!())?;
        while let Some(entry) = entries.next_entry().await.map_error(dbg_loc!())? {
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "bin" {
                        index_files.push(path);
                    }
                }
            }
        }
    } else if target_path.is_file() && target_path.extension().map_or(false, |e| e == "bin") {
        index_files.push(target_path);
    } else {
        return Err(format!(
            "Target {} is not a directory or .index file",
            args.target_dir
        )
        .into_error(dbg_loc!()));
    }

    if index_files.is_empty() {
        return Err(format!("No .index files found in {}", args.target_dir).into_error(dbg_loc!()));
    }

    let search_args = SearchArgs {
        index_files,
        workers: args.workers,
    };

    let (sender, mut recvr) = tokio::sync::mpsc::channel(100);
    let query = args.substring.as_bytes().to_vec();

    // Spawn search task
    let search_task =
        tokio::spawn(async move { search_in_index_files(search_args,  query, sender).await });

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
