use super::control::{SchedulerControlHandle, SchedulerError, SchedulerResponse};
use std::sync::Arc;
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
    Invalid { raw: String, hint: String },
}

/// 简易引号感知 tokenizer
///
/// 支持双引号包裹含空格的参数，如 `submit "C:\my path\job.json"`
/// 不支持转义引号（`\"`），满足当前场景即可。
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ' ' | '\t' if !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

impl ReplCommand {
    pub fn parse(input: &str) -> Self {
        let tokens = tokenize(input);
        let cmd = match tokens.first() {
            Some(c) => c.as_str(),
            None => {
                return Self::Invalid {
                    raw: String::new(),
                    hint: String::new(),
                }
            }
        };

        match cmd {
            "status" => match tokens.get(1) {
                None => Self::Status { job_id: None },
                Some(id) => Self::Status {
                    job_id: Some(id.clone()),
                },
            },
            "submit" => match tokens.get(1) {
                Some(path) => Self::Submit {
                    path: path.clone(),
                },
                None => Self::Invalid {
                    raw: "submit".to_string(),
                    hint: "Usage: submit <path>".to_string(),
                },
            },
            "cancel" => match tokens.get(1) {
                Some(id) => Self::Cancel {
                    job_id: id.clone(),
                },
                None => Self::Invalid {
                    raw: "cancel".to_string(),
                    hint: "Usage: cancel <job_id>".to_string(),
                },
            },
            "exit" | "quit" => Self::Exit,
            _ => Self::Invalid {
                raw: cmd.to_string(),
                hint: format!(
                    "(error) Unknown command '{}'. Available commands: status [job_id], submit <path>, cancel <job_id>, exit",
                    cmd
                ),
            },
        }
    }
}

/// 交互式 REPL 循环
///
/// 在独立线程中运行，通过 channel 将用户命令发送给 TaskScheduler。
pub struct ReplLoop {
    control: SchedulerControlHandle,
    cancel: CancellationToken,
}

impl ReplLoop {
    pub fn new(control: SchedulerControlHandle, cancel: CancellationToken) -> Self {
        Self { control, cancel }
    }

    /// 阻塞式 readline 循环，应运行在独立线程中。
    pub fn run(self: Arc<Self>) {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!("Failed to init REPL runtime: {}", e);
                return;
            }
        };

        let mut rl = match rustyline::DefaultEditor::new() {
            Ok(editor) => editor,
            Err(e) => {
                warn!("Failed to init readline, REPL disabled: {}", e);
                return;
            }
        };

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
                    let is_exit = matches!(cmd, ReplCommand::Exit);
                    let output = rt.block_on(self.execute_command(cmd));
                    if !output.is_empty() {
                        println!("{}", output);
                    }
                    if is_exit {
                        break;
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    let output = rt.block_on(self.execute_command(ReplCommand::Exit));
                    if !output.is_empty() {
                        println!("{}", output);
                    }
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

    async fn execute_command(&self, cmd: ReplCommand) -> String {
        match cmd {
            ReplCommand::Status { job_id } => self
                .control
                .query_tasks(job_id)
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Submit { path } => self
                .control
                .submit_task(path.into())
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Cancel { job_id } => self
                .control
                .cancel_task(job_id)
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Exit => self
                .control
                .shutdown()
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Invalid { raw: _, hint } => hint,
        }
    }
}

fn format_repl_response(response: SchedulerResponse) -> String {
    match response {
        SchedulerResponse::Tasks { tasks, repl_alive } => {
            if tasks.is_empty() {
                return "No tasks.".to_string();
            }

            let mut lines = vec![format!(
                "repl: {}",
                if repl_alive { "alive" } else { "dead" }
            )];
            lines.push(format!(
                "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
                "job_id", "phase", "cdc", "read", "written", "elapsed"
            ));
            lines.push("-".repeat(74));

            for task in tasks {
                let (read, written, elapsed) = match task.stats {
                    Some(stats) => (
                        stats.records_read.to_string(),
                        stats.records_written.to_string(),
                        format!("{:.1}s", stats.elapsed_secs),
                    ),
                    None => ("-".to_string(), "-".to_string(), "-".to_string()),
                };
                lines.push(format!(
                    "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
                    task.job_id, task.phase, task.is_cdc, read, written, elapsed
                ));
            }

            lines.join("\n")
        }
        SchedulerResponse::TaskSubmitted { job_id } => format!("Job '{}' submitted.", job_id),
        SchedulerResponse::TaskCancelled { job_id } => {
            format!("Job '{}' cancel signal sent.", job_id)
        }
        SchedulerResponse::ShutdownRequested => "Scheduler shutdown requested.".to_string(),
    }
}

fn format_repl_error(error: SchedulerError) -> String {
    error.message()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("status"), vec!["status"]);
        assert_eq!(tokenize("cancel job_1"), vec!["cancel", "job_1"]);
    }

    #[test]
    fn tokenize_quoted_path() {
        assert_eq!(
            tokenize(r#"submit "C:\my path\job.json""#),
            vec!["submit", r"C:\my path\job.json"]
        );
    }

    #[test]
    fn tokenize_empty() {
        assert_eq!(tokenize(""), Vec::<String>::new());
        assert_eq!(tokenize("   "), Vec::<String>::new());
    }

    #[test]
    fn parse_commands() {
        assert!(matches!(ReplCommand::parse("exit"), ReplCommand::Exit));
        assert!(matches!(ReplCommand::parse("quit"), ReplCommand::Exit));
        assert!(matches!(
            ReplCommand::parse("status"),
            ReplCommand::Status { job_id: None }
        ));
        assert!(matches!(
            ReplCommand::parse("cancel"),
            ReplCommand::Invalid { .. }
        ));
        assert!(matches!(
            ReplCommand::parse("foo bar"),
            ReplCommand::Invalid { .. }
        ));
    }
}
