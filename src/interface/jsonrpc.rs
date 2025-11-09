//! Interact with the core functionality via JSON-RPC.
//! Currently supports stdio transport.

use super::BuildIndexIn;
use super::BuildIndexOut;
use jsonrpc_core::IoHandler;
use jsonrpc_core::Params;
use serde_json::Value;

pub async fn start_loop() {
    let mut io = IoHandler::new();
    io.add_method("build_index", |params: Params| async move {
        let build_index_in: BuildIndexIn = params.parse().unwrap();
        Ok(serde_json::to_value(BuildIndexOut { task_id: 1 }).unwrap())
    });
}
