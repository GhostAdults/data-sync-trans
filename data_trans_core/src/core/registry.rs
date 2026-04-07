//! 数据源注册表
//!
//! 核心引擎通过 GlobalRegistry 动态创建 Reader/Writer。
//! 具体数据源的注册由各 Reader/Writer crate 自行完成。

pub use data_trans_common::interface::GlobalRegistry;

use std::sync::OnceLock;

static INITIALIZED: OnceLock<()> = OnceLock::new();

/// 确保注册表已初始化 ini
pub fn ensure_initialized() {
    INITIALIZED.get_or_init(|| {
        let registry = GlobalRegistry::instance();
        data_trans_reader::register(registry);
        data_trans_writer::register(registry);
        // 在这里引用新的数据源crate...
    });
}
