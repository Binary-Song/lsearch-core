use crate::core::Error;
use crate::prelude::IntoError;
use crate::prelude::MapError;
use crate::core::index_directory;
use crate::core::IndexArgs;
use num_cpus;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitCode;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;



#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SearchResponse {
    OccurenceFound {
        file: String,
        line: usize,
        column: usize,
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
#[derive(Deserialize)]
pub struct SearchRequest {
    target_dir: String,
    substring: String,
}
