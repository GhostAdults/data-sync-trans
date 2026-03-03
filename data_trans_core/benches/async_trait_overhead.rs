use std::hint::black_box;
use async_trait::async_trait;
use tokio::runtime::Runtime;

// ==========================================
// 测试 1: async_trait 版本
// ==========================================

#[async_trait]
trait AsyncTraitJob {
    async fn execute(&self, n: usize) -> usize;
}

struct AsyncTraitImpl;

#[async_trait]
impl AsyncTraitJob for AsyncTraitImpl {
    async fn execute(&self, n: usize) -> usize {
        // 模拟一些异步工作
        tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
        n * 2
    }
}

// ==========================================
// 测试 2: 原生 async fn（无 trait）
// ==========================================

struct NativeImpl;

impl NativeImpl {
    async fn execute(&self, n: usize) -> usize {
        // 相同的工作
        tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
        n * 2
    }
}

// ==========================================
// 测试 3: 手动 Box 版本（模拟 async_trait）
// ==========================================

trait ManualBoxJob {
    fn execute(&self, n: usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + '_>>;
}

struct ManualBoxImpl;

impl ManualBoxJob for ManualBoxImpl {
    fn execute(&self, n: usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            n * 2
        })
    }
}

// ==========================================
// 基准测试
// ==========================================

fn main() {
    let rt = Runtime::new().unwrap();
    let iterations = 10_000;

    println!("🔬 async_trait 性能测试 ({} 次迭代)\n", iterations);

    // 测试 1: async_trait
    let start = std::time::Instant::now();
    rt.block_on(async {
        let job: &dyn AsyncTraitJob = &AsyncTraitImpl;
        for i in 0..iterations {
            black_box(job.execute(black_box(i)).await);
        }
    });
    let async_trait_time = start.elapsed();

    // 测试 2: 原生 async fn
    let start = std::time::Instant::now();
    rt.block_on(async {
        let job = NativeImpl;
        for i in 0..iterations {
            black_box(job.execute(black_box(i)).await);
        }
    });
    let native_time = start.elapsed();

    // 测试 3: 手动 Box
    let start = std::time::Instant::now();
    rt.block_on(async {
        let job: &dyn ManualBoxJob = &ManualBoxImpl;
        for i in 0..iterations {
            black_box(job.execute(black_box(i)).await);
        }
    });
    let manual_box_time = start.elapsed();

    // 结果
    println!("📊 测试结果:");
    println!("   async_trait:     {:?} ({:.2} μs/iter)", async_trait_time, async_trait_time.as_micros() as f64 / iterations as f64);
    println!("   原生 async fn:   {:?} ({:.2} μs/iter)", native_time, native_time.as_micros() as f64 / iterations as f64);
    println!("   手动 Box:        {:?} ({:.2} μs/iter)", manual_box_time, manual_box_time.as_micros() as f64 / iterations as f64);

    println!("\n📈 相对开销:");
    let overhead_pct = ((async_trait_time.as_nanos() as f64 / native_time.as_nanos() as f64) - 1.0) * 100.0;
    println!("   async_trait vs 原生: +{:.2}%", overhead_pct);

    let overhead_ns = (async_trait_time.as_nanos() as f64 - native_time.as_nanos() as f64) / iterations as f64;
    println!("   每次调用额外开销: {:.2} ns", overhead_ns);
}
