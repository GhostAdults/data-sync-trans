use chrono::Local;
use clap::Parser;
use data_trans_core::core::cli::{run_cli, Cli};
use data_trans_core::init_and_watch_config;
use std::io::Write;
use std::sync::Mutex;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;

struct ChronoLocalTimer;

impl FormatTime for ChronoLocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

fn main() {
    let cli = Cli::parse();
    init_and_watch_config();

    let log_file = std::fs::File::create("app.log").expect("无法创建 app.log");
    let log_file = Mutex::new(log_file);

    tracing_subscriber::fmt()
        .with_timer(ChronoLocalTimer)
        .with_writer(move || {
            let file = log_file
                .lock()
                .unwrap()
                .try_clone()
                .expect("clone log file");
            Box::new(std::io::BufWriter::new(file)) as Box<dyn Write + Send>
        })
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    run_cli(cli.command);
}
