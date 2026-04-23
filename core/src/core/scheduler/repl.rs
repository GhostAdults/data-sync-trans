use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const PROMPT: &str = "relus_cli> ";

/// REPL 命令
#[derive(Debug)]
pub enum ReplCommand {
    Status { job_id: Option<String> },
    Submit { path: String },
    Cancel { job_id: String },
    Exit,
    Unknown(String),
}

impl ReplCommand {
    pub fn parse(input: &str) -> Self {
        let tokens: Vec<&str> = input.split_whitespace().collect();
        match tokens.as_slice() {
            ["status"] => Self::Status { job_id: None },
            ["status", id] => Self::Status {
                job_id: Some((*id).to_string()),
            },
            ["submit", path] => Self::Submit {
                path: (*path).to_string(),
            },
            ["submit"] => Self::Unknown("Usage: submit <path>".to_string()),
            ["cancel", id] => Self::Cancel {
                job_id: (*id).to_string(),
            },
            ["cancel"] => Self::Unknown("Usage: cancel <job_id>".to_string()),
            ["exit"] | ["quit"] => Self::Exit,
            other => Self::Unknown(other.join(" ")),
        }
    }
}

/// 交互式 REPL 循环
///
/// 在 `spawn_blocking` 中运行，通过 channel 将用户命令发送给 TaskScheduler。
pub struct ReplLoop {
    cmd_tx: mpsc::Sender<ReplCommand>,
    cancel: CancellationToken,
}

impl ReplLoop {
    pub fn new(cmd_tx: mpsc::Sender<ReplCommand>, cancel: CancellationToken) -> Self {
        Self { cmd_tx, cancel }
    }

    /// 阻塞式 readline 循环，应运行在 `spawn_blocking` 中。
    pub fn run(self: Arc<Self>) {
        let mut rl = rustyline::DefaultEditor::new().unwrap_or_else(|e| {
            eprintln!("Failed to init readline: {}", e);
            std::process::exit(1);
        });

        while !self.cancel.is_cancelled() {
            let line = rl.readline(PROMPT);
            match line {
                Ok(input) => {
                    let trimmed = input.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let _ = rl.add_history_entry(trimmed);
                    let cmd = ReplCommand::parse(trimmed);
                    if self.cmd_tx.blocking_send(cmd).is_err() {
                        break;
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    // Ctrl+C → 发送 Exit
                    let _ = self.cmd_tx.blocking_send(ReplCommand::Exit);
                    break;
                }
                Err(rustyline::error::ReadlineError::Eof) => {
                    break;
                }
                Err(e) => {
                    warn!("readline error: {}", e);
                    break;
                }
            }
        }
    }
}
