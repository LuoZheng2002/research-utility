use std::io;
use std::time::Duration;

use research_utility::message::{MyLogMessage, Severity};
use research_utility::log_message::{
    log_key_value_pair, log_master_progress, log_message, log_worker_progress
};
use research_utility::progress_screen::ProgressScreen;

#[tokio::main]
async fn main() -> io::Result<()> {
    let num_workers = 6;
    let steps_per_worker = 40;

    let log_file = Some("test.log".to_string());
    ProgressScreen::initialize("Progress Screen Preview".to_string(), true, log_file).await?;
    simulate_progress(num_workers, steps_per_worker).await;
    ProgressScreen::shutdown().await
}

async fn simulate_progress(num_workers: usize, steps_per_worker: usize) {
    let total_ticks = steps_per_worker + (num_workers * 4);

    for tick in 0..=total_ticks {
        let phase = if tick < total_ticks / 3 {
            "indexing"
        } else if tick < (total_ticks * 2) / 3 {
            "processing"
        } else {
            "finalizing"
        };

        let mut sum_progress = 0.0;
        let mut finished_workers = 0usize;

        for worker_id in 0..num_workers {
            let stagger = worker_id * 4;
            let effective = tick.saturating_sub(stagger).min(steps_per_worker);
            let progress = (effective as f32) / (steps_per_worker as f32);

            if progress >= 1.0 {
                finished_workers += 1;
            }

            let label = if progress >= 1.0 {
                "100% (done)".to_string()
            } else {
                format!("{:>3}% ({phase})", (progress * 100.0).round() as u32)
            };

            log_worker_progress(format!("endpoint-{}", worker_id + 1), progress, label);

            sum_progress += progress;
        }

        let master_progress = sum_progress / (num_workers as f32);
        let master_label = if master_progress >= 1.0 {
            "100% (all endpoints done)".to_string()
        } else {
            format!("{:>3}% ({phase})", (master_progress * 100.0).round() as u32)
        };

        log_master_progress(master_progress, master_label);

        let status = if master_progress >= 1.0 {
            "completed"
        } else {
            "running"
        };
        log_key_value_pair(
            "status".to_string(),
            format!("{status} ({finished_workers}/{num_workers} endpoints done)"),
        );
        log_key_value_pair("phase".to_string(), phase.to_string());
        log_key_value_pair("tick".to_string(), format!("{tick}/{total_ticks}"));

        let severity = if tick % 17 == 0 {
            Severity::Error
        } else if tick % 7 == 0 {
            Severity::Warning
        } else {
            Severity::Info
        };
        let log_line = format!(
            "tick={tick} phase={phase} master={:.0}% workers_done={finished_workers}/{num_workers}",
            master_progress * 100.0
        );
        log_message(MyLogMessage::Line {
                message: log_line,
                severity,
            });

        tokio::time::sleep(Duration::from_millis(120)).await;
    }
}
