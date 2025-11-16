pub mod core;
pub mod interfaces;
mod prelude;
#[cfg(test)]
mod test;

use std::process::ExitCode;
use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
pub struct CommandLineArgs {
    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    Json {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 1114)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = CommandLineArgs::parse();



    let ok = match &args.mode {
        Mode::Json { .. } => interfaces::json_lines::entry_point(&args).await,
    };
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
