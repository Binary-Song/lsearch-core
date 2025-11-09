//! Defines how the user interact with the core module.
//! Currently supports JSON-RPC via stdio. May support others in the future.

pub mod jsonrpc;

// =======================================================
// All interfaces share these input/output structures
// =======================================================

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct BuildIndexIn {
    target_dir: String,
    includes: Vec<String>,
    excludes: Vec<String>,
    read_chunk_size: usize,
    gram_size: usize,
    channel_capacity: usize,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct BuildIndexOut {
    /// Use the task_id to poll for progress or get the final result.
    task_id: usize,
}
