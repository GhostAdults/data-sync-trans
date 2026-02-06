use clap::Parser;
use data_trans_core::core::cli::{run_cli, Cli};

fn main() {
    let cli = Cli::parse();
    run_cli(cli.command);
}
