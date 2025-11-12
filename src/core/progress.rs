#[derive(Debug)]
pub enum Progress {
    GlobUpdated {
        entries: usize,
    },
    IndexAdded {
        finished_entries: usize,
        total_entries: usize,
    },
    Writing,
    FileSearched {
        finished_files: usize,
        total_files: usize,
    }
}
