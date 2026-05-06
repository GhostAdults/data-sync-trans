use chrono::Local;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;

struct ChronoLocalTimer;

impl FormatTime for ChronoLocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// 在 exe 所在目录下创建 `logs/YYYY-MM-DD.log`，并以本地时间格式初始化 tracing 日志。
///
/// # Panics
///
/// 当无法创建日志文件时 panic。
pub fn init_file_logger() {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));

    let log_dir = exe_dir.join("logs");
    if let Err(err) = std::fs::create_dir_all(&log_dir) {
        eprintln!("无法创建 logs 目录 {}: {}", log_dir.display(), err);
        return;
    }

    let log_name = format!("{}.log", Local::now().format("%Y-%m-%d"));
    let log_path = log_dir.join(&log_name);
    let log_file = std::fs::File::create(&log_path)
        .unwrap_or_else(|_| panic!("无法创建日志文件: {}", log_path.display()));
    let log_file = Mutex::new(log_file);

    tracing_subscriber::fmt()
        .with_timer(ChronoLocalTimer)
        .with_writer(move || {
            let file = log_file.lock().unwrap();
            match file.try_clone() {
                Ok(file) => Box::new(std::io::BufWriter::new(file)) as Box<dyn Write + Send>,
                Err(err) => {
                    eprintln!("无法克隆日志文件句柄: {}", err);
                    Box::new(std::io::sink()) as Box<dyn Write + Send>
                }
            }
        })
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
}
