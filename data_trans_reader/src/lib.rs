pub mod api_reader;
pub mod database_reader;
pub mod rdbms_reader_util;

pub use api_reader::ApiJob;
pub use data_trans_common::interface;
pub use database_reader::DatabaseJob;
pub use rdbms_reader_util::rdbms_reader::{
    count_total_records, execute_query_stream, DbRowStream, RdbmsConfig, RdbmsJob,
};

/// 注册本 crate 提供的所有 Reader 类型
pub fn register(registry: &interface::GlobalRegistry) {
    registry.register_reader("api", |config| Ok(Box::new(ApiJob::new(config))));

    registry.register_reader("database", |config| {
        let job = DatabaseJob::new(config)?;
        Ok(Box::new(job))
    });
}
