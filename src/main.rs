pub mod core;
pub mod interfaces;
mod prelude;

use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;

#[derive(Parser, Debug)]
pub struct CommandLineArgs {
    #[command(subcommand)]
    pub mode: Mode,
    #[arg(long)]
    pub init_console_subscriber: bool,
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
