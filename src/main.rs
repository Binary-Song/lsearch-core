mod core;
mod interface;

use serde_json::Value;
use std::process::ExitCode;
use std::{
    fs::read,
    io::{BufRead, BufReader},
};

#[tokio::main]
async fn main() -> ExitCode {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let reader = BufReader::new(stdin);
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => {
                return ExitCode::FAILURE;
            }
        };

        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
    }

    todo!()
}
