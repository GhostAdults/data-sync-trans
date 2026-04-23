use clap::Parser;
use relus_core::core::cli::{run_cli, Cli};
use relus_core::init_and_watch_config;

/// 数据同步 cli 入口
fn main() {
    let cli: Cli = Cli::parse();
    init_and_watch_config();

    relus_common::logging::init_file_logger();

    run_cli(cli.command);
}
