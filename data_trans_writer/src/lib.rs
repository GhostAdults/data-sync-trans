pub mod database_writer;
pub mod rdbms_writer_util;

pub use data_trans_common::{interface, pipeline};
pub use database_writer::DatabaseJob;
pub use rdbms_writer_util::rdbms_writer::{RdbmsConfig, RdbmsJob, RowWriter};

/// 注册本 crate 提供的所有 Writer 类型
pub fn register(registry: &interface::GlobalRegistry) {
    registry.register_writer("database", |config| {
        let job = DatabaseJob::new(config)?;
        Ok(Box::new(job))
    });
}
