//! 同步任务实时进度条
//!
//! 基于 indicatif MultiProgress，Reader/Writer 通过 ProgressBar::inc 实时递增。

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// 进度条上下文
pub struct ProgressContext {
    multi: MultiProgress,
    pub reader_bar: ProgressBar,
    pub spacer: ProgressBar,
    pub writer_bar: ProgressBar,
    pub total_records: usize,
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/green} {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("█ ")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg}").unwrap()
}

pub fn create_progress_bars(total_records: usize) -> ProgressContext {
    let multi = MultiProgress::new();
    multi.set_draw_target(ProgressDrawTarget::stderr()); //防止进度条被日志输出干扰，保持在终端底部显示
    let sty = bar_style();

    let (reader_bar, spacer, writer_bar) = if total_records > 0 {
        let rb: ProgressBar = multi.add(ProgressBar::new(total_records as u64));
        rb.set_style(sty.clone());
        rb.set_prefix("Reader");
        rb.set_message("reading");

        let spacer = multi.insert_after(&rb, ProgressBar::new(1));
        spacer.set_style(spinner_style());
        spacer.set_message(" ");

        let wb = multi.insert_after(&spacer, ProgressBar::new(total_records as u64));
        wb.set_style(sty);
        wb.set_prefix("Writer");
        wb.set_message("writing");

        (rb, spacer, wb)
    } else {
        let rb = multi.add(ProgressBar::new_spinner());
        rb.set_style(spinner_style());
        rb.set_prefix("Reader");
        rb.set_message("reading");

        let spacer = multi.insert_after(&rb, ProgressBar::new(0));
        spacer.set_style(ProgressStyle::with_template(" ").unwrap());

        let wb = multi.insert_after(&spacer, ProgressBar::new_spinner());
        wb.set_style(spinner_style());
        wb.set_prefix("Writer");
        wb.set_message("writing");

        (rb, spacer, wb)
    };

    ProgressContext {
        multi,
        reader_bar,
        spacer,
        writer_bar,
        total_records,
    }
}

impl ProgressContext {
    pub fn finish(&self) {
        self.reader_bar.finish_with_message("Done.");
        self.writer_bar.finish_with_message("Done.");
        self.spacer
            .finish_with_message("--------------------------------------------");

        // summary
        self.multi.println("All tasks completed!").unwrap();
    }
}
