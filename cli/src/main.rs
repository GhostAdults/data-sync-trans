use clap::Parser;
use relus_core::core::cli::{run_cli, Cli};
use std::process::ExitCode;

/// 数据同步 cli 入口
fn main() -> ExitCode {
    let cli: Cli = Cli::parse();

    relus_common::logging::init_file_logger();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Failed to initialize runtime: {}", e);
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run_cli(cli.command)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}
