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
}
