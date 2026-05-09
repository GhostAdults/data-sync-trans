//! 全局常量定义

pub mod schema {
    /// Schema 缓存默认目录
    pub const DEFAULT_SCHEMA_CACHE_DIR: &str = "./schema_cache";

    /// Schema 版本号初始值
    pub const SCHEMA_VERSION_INITIAL: u64 = 1;
}

pub mod key {
    /// 主键列默认名称
    pub const DEFAULT_KEY_COLUMN: &str = "id";
    pub const SPLIT_FACTOR: usize = 3;
}

pub mod pipeline {
    /// 默认批处理大小
    pub const DEFAULT_BATCH_SIZE: usize = 1000;

    /// 默认缓冲区大小
    pub const DEFAULT_BUFFER_SIZE: usize = 1000;

    /// 默认 Reader 线程数
    pub const DEFAULT_READER_THREADS: usize = 4;

    /// 默认 Channel 并发数
    pub const DEFAULT_CHANNEL_NUMBER: usize = 20;

    /// 默认每个 TaskGroup 内 Channel 并发数
    pub const DEFAULT_PER_GROUP_CHANNEL: usize = 10;

    /// 默认 Writer 线程数
    pub const DEFAULT_WRITER_THREADS: usize = 4;
}
