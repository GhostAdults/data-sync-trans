//! Writer 任务切分工具

use std::sync::Arc;

use data_trans_common::job_config::DbConfig;
use data_trans_common::JobConfig;

use data_trans_common::interface::{SplitWriterResult, WriteMode, WriteTask};

/// 切分 Writer 任务
pub fn do_split(original_config: &Arc<JobConfig>, advice_number: usize) -> SplitWriterResult {
    let mode = original_config
        .mode
        .as_deref()
        .map(WriteMode::from_str)
        .unwrap_or(WriteMode::Insert);

    let batch_size = original_config.batch_size.unwrap_or(100);

    let db_config = DbConfig::default();
    let use_transaction = db_config.use_transaction.unwrap_or(false);

    //这里直接按照advice number进行切分就好，
    let tasks: Vec<WriteTask> = (0..advice_number)
        .map(|i| WriteTask {
            task_id: i,
            config: Arc::clone(original_config),
            mode,
            use_transaction,
            batch_size,
        })
        .collect();

    SplitWriterResult { tasks }
}
