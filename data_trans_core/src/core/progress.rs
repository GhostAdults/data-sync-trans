//! 同步任务实时进度条
//!
//! 基于 indicatif MultiProgress，Reader/Writer 通过 ProgressBar::inc 实时递增。

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// 进度条上下文
pub struct ProgressContext {
    multi: MultiProgress,
    pub reader_bar: ProgressBar,
    pub writer_bar: ProgressBar,
    pub total_records: usize,
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("##-")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("[{elapsed_precise}] {prefix:>8} {pos} rows {msg}").unwrap()
}

pub fn create_progress_bars(total_records: usize) -> ProgressContext {
    let multi = MultiProgress::new();
    let sty = bar_style();
    let spin_sty = spinner_style();

    let (reader_bar, writer_bar) = if total_records > 0 {
        let rb = multi.add(ProgressBar::new(total_records as u64));
        rb.set_style(sty.clone());
        rb.set_prefix("Reader");
        rb.set_message("reading");

        let wb = multi.add(ProgressBar::new(total_records as u64));
        wb.set_style(sty);
        wb.set_prefix("Writer");
        wb.set_message("writing");

        (rb, wb)
    } else {
        let rb = multi.add(ProgressBar::new_spinner());
        rb.set_style(spin_sty.clone());
        rb.set_prefix("Reader");
        rb.set_message("reading");

        let wb = multi.add(ProgressBar::new_spinner());
        wb.set_style(spin_sty);
        wb.set_prefix("Writer");
        wb.set_message("writing");

        (rb, wb)
    };

    ProgressContext {
        multi,
        reader_bar,
        writer_bar,
        total_records,
    }
}

impl ProgressContext {
    pub fn finish(self, elapsed_secs: f64) {
        let read = self.reader_bar.position() as usize;
        let written = self.writer_bar.position() as usize;

        self.reader_bar.finish_with_message("done");
        self.writer_bar.finish_with_message("done");

        let throughput = if elapsed_secs > 0.0 {
            written as f64 / elapsed_secs
        } else {
            0.0
        };

        self.multi.clear().ok();
        println!(
            "完成: 读取 {} 条, 写入 {} 条, 耗时 {:.2}s, 吞吐 {:.0} 条/秒",
            read, written, elapsed_secs, throughput
        );
    }
}
