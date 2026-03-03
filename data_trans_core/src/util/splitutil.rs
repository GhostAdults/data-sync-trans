use data_trans_reader::{Task, JobSplitResult};

pub fn split_by_total(total_records: usize, reader_threads: usize) -> JobSplitResult {
    let task_size = if total_records == 0 {
        0
    } else {
        (total_records + reader_threads - 1) / reader_threads
    };

    let mut tasks = Vec::new();
    for i in 0..reader_threads {
        let offset = i * task_size;
        if offset >= total_records {
            break;
        }
        let limit = task_size.min(total_records - offset);

        tasks.push(Task {
            task_id: i,
            offset,
            limit,
        });
    }

    JobSplitResult {
        total_records,
        tasks,
    }
}
