/// 全局常量定义

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

pub mod db {
    /// 默认最大连接数
    pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

    /// 默认连接超时时间（秒）
    pub const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 30;

    /// 基础查询模板: SELECT {columns} FROM {table}
    pub const SQL_SELECT: &str = "SELECT {} FROM {}";

    /// 带 WHERE 条件的查询模板: SELECT {columns} FROM {table} WHERE {where}
    pub const SQL_SELECT_WHERE: &str = "SELECT {} FROM {} WHERE {}";

    /// 带 LIMIT/OFFSET 的查询模板: SELECT {columns} FROM {table} LIMIT {limit} OFFSET {offset}
    pub const SQL_SELECT_LIMIT_OFFSET: &str = "SELECT {} FROM {} LIMIT {} OFFSET {}";

    /// 带 WHERE 和 LIMIT/OFFSET 的查询模板
    pub const SQL_SELECT_WHERE_LIMIT_OFFSET: &str = "SELECT {} FROM {} WHERE {} LIMIT {} OFFSET {}";

    /// 范围查询模板 (开区间): SELECT {columns} FROM {table} WHERE {pk} >= {min} AND {pk} < {max}
    pub const SQL_SELECT_RANGE_OPEN: &str = "SELECT {} FROM {} WHERE {} >= {} AND {} < {}";

    /// 范围查询模板 (闭区间): SELECT {columns} FROM {table} WHERE {pk} >= {min} AND {pk} <= {max}
    pub const SQL_SELECT_RANGE_CLOSED: &str = "SELECT {} FROM {} WHERE {} >= {} AND {} <= {}";

    /// NULL 条件查询模板: SELECT {columns} FROM {table} WHERE {pk} IS NULL
    pub const SQL_SELECT_NULL: &str = "SELECT {} FROM {} WHERE {} IS NULL";
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
