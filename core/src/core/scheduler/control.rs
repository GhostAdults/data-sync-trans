use super::cmd::{Schedule, TaskInfo};
use relus_common::job_config::JobConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub type SchedulerResult = Result<SchedulerResponse, SchedulerError>;

#[derive(Debug)]
pub enum SchedulerCommand {
    QueryTasks {
        job_id: Option<String>,
        reply: oneshot::Sender<SchedulerResult>,
    },
    SubmitTask {
        job_id: Option<String>,
        path: PathBuf,
        reply: oneshot::Sender<SchedulerResult>,
    },
    CancelTask {
        job_id: String,
        reply: oneshot::Sender<SchedulerResult>,
    },
    Shutdown {
        reply: oneshot::Sender<SchedulerResult>,
    },
}

#[derive(Debug, Clone)]
pub struct SchedulerControlHandle {
    tx: mpsc::Sender<SchedulerCommand>,
}

impl SchedulerControlHandle {
    #[cfg(test)]
    pub fn from_sender(tx: mpsc::Sender<SchedulerCommand>) -> Self {
        Self { tx }
    }

    pub fn new(tx: mpsc::Sender<SchedulerCommand>) -> Self {
        Self { tx }
    }

    pub async fn query_tasks(
        &self,
        job_id: Option<String>,
    ) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::QueryTasks { job_id, reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn submit_task(&self, path: PathBuf) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::SubmitTask {
                job_id: None,
                path,
                reply,
            })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn cancel_task(&self, job_id: String) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::CancelTask { job_id, reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn shutdown(&self) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::Shutdown { reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulerResponse {
    Tasks {
        tasks: Vec<TaskInfo>,
        repl_alive: bool,
    },
    TaskSubmitted {
        job_id: String,
    },
    TaskCancelled {
        job_id: String,
    },
    ShutdownRequested,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerError {
    JobNotFound { job_id: String },
    JobAlreadyExists { job_id: String },
    InvalidConfig { message: String },
    MaxConcurrencyReached { running: usize, limit: usize },
    SchedulerUnavailable,
    Internal { message: String },
}

impl SchedulerError {
    pub fn message(&self) -> String {
        match self {
            SchedulerError::JobNotFound { job_id } => format!("Job '{}' not found.", job_id),
            SchedulerError::JobAlreadyExists { job_id } => {
                format!("Job '{}' already exists.", job_id)
            }
            SchedulerError::InvalidConfig { message } => message.clone(),
            SchedulerError::MaxConcurrencyReached { running, limit } => {
                format!("Max concurrency reached ({}/{}).", running, limit)
            }
            SchedulerError::SchedulerUnavailable => "Scheduler is unavailable.".to_string(),
            SchedulerError::Internal { message } => message.clone(),
        }
    }
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for SchedulerError {}

pub fn load_job_config_from_path(
    path: &PathBuf,
) -> Result<(String, Arc<JobConfig>, Schedule), SchedulerError> {
    let data = std::fs::read_to_string(path).map_err(|e| SchedulerError::InvalidConfig {
        message: format!("File not found: {} ({})", path.display(), e),
    })?;

    let config: JobConfig =
        serde_json::from_str(&data).map_err(|e| SchedulerError::InvalidConfig {
            message: format!("Parse failed: {}", e),
        })?;

    let job_id = config
        .job_id
        .clone()
        .or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let schedule = config
        .schedule
        .as_ref()
        .map(Schedule::from_config)
        .transpose()
        .map_err(|message| SchedulerError::InvalidConfig { message })?
        .unwrap_or_default();

    Ok((job_id, Arc::new(config), schedule))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_error_messages_are_structured() {
        assert_eq!(
            SchedulerError::JobNotFound {
                job_id: "job-1".to_string(),
            }
            .message(),
            "Job 'job-1' not found."
        );
        assert_eq!(
            SchedulerError::MaxConcurrencyReached {
                running: 3,
                limit: 3,
            }
            .message(),
            "Max concurrency reached (3/3)."
        );
    }
}
