#[allow(unused_imports)]
use crate::prelude::*;
use std::path::PathBuf;
#[derive(Debug)]
pub enum Progress {
    GlobUpdated {
        entries: usize,
    },
    GlobDone,
    StratificationDone,
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
    Done,
}
