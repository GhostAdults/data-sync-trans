use clap::Parser;
use data_trans_core::core::cli::{run_cli, Cli};
use data_trans_core::init_and_watch_config;

fn main() {
    let cli = Cli::parse();
    init_and_watch_config();
    run_cli(cli.command);
}
