use clap::Parser;
use data_trans_core::core::cli::{run_cli, Cli};
use data_trans_core::init_and_watch_config;

fn main() {
    let cli = Cli::parse();
    init_and_watch_config();
    run_cli(cli.command);
}

#[cfg(test)]
mod tests {
    use data_trans_common::job_config::JobConfig;
    use data_trans_core::core::pipeline::PipelineConfig;
    use data_trans_core::{init_and_watch_config, init_system_config};

    /// 测试系统配置加载 + PipelineConfig 读取
    ///
    /// 验证 default.config.json 中的 pipeline 段被正确读取到 PipelineConfig
    #[test]
    fn test_config_load() {
        // 初始化系统配置
        let _ = init_and_watch_config();

        // 用默认 JobConfig 构造 PipelineConfig（系统配置优先）
        let job_config = JobConfig::default_test();
        let pipeline_config = PipelineConfig::from_system_config(&job_config);

        println!("PipelineConfig 加载结果:");
        println!("  reader_threads: {}", pipeline_config.reader_threads);
        println!("  buffer_size: {}", pipeline_config.buffer_size);
        println!("  batch_size: {}", pipeline_config.batch_size);
        println!("  use_transaction: {}", pipeline_config.use_transaction);

        // 验证 default.config.json 中 pipeline 段的默认值
        assert_eq!(pipeline_config.reader_threads, 8, "reader_threads 应为 8");
        assert_eq!(pipeline_config.buffer_size, 1000, "buffer_size 应为 1000");
        assert_eq!(pipeline_config.batch_size, 100, "batch_size 应为 100");
        assert!(pipeline_config.use_transaction, "use_transaction 应为 true");
    }

    /// 测试系统配置优先级
    #[test]
    fn test_config_priority() {
        let _ = init_system_config();

        let job_config = JobConfig::default_test();
        let pipeline_config = PipelineConfig::from_system_config(&job_config);

        // 系统配置应生效
        assert_eq!(
            pipeline_config.reader_threads, 4,
            "系统配置 reader_threads 应为 4"
        );
        assert_eq!(
            pipeline_config.buffer_size, 1000,
            "系统配置 buffer_size 应为 1000"
        );
        assert_eq!(pipeline_config.batch_size, 100, "系统配置 batch_size 应为 100");
    }
}
