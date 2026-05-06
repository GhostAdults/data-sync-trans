use clap::Parser;
use relus_core::core::cli::{run_cli, Cli};
use std::process::ExitCode;

/// 数据同步 cli 入口
fn main() -> ExitCode {
    let cli: Cli = Cli::parse();

    relus_common::logging::init_file_logger();
    let default_parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let worker_limit = std::cmp::min(default_parallelism, 16);
    let blocking_limit = 4 * worker_limit;

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_limit)
        .max_blocking_threads(blocking_limit)
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
