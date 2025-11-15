use std::path::PathBuf;
use crate::prelude::*;
#[derive(Debug)]
pub enum Progress {
    GlobUpdated {
        entries: usize,
    },
    GlobDone,
    IndexAdded {
        finished_entries: usize,
        total_entries: usize,
    },
    IndexWritten {
        output_path: PathBuf,
    },
    FileSearched {
        finished_files: usize,
        total_files: usize,
    },
    ErrorOccurred {
        message: String,
    },
}
