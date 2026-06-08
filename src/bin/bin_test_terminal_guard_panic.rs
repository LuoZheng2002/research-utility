use std::io;
use std::time::Duration;

use clap::Parser;
use research_utility::progress_tui_logger::{
    ProgressTuiLogger, log_key_value_pair, log_worker_progress,
};

#[derive(Debug, Parser)]
#[command(name = "bin_test_terminal_guard_panic")]
#[command(about = "Triggers a panic path in progress TUI handling")]
struct Cli {
    #[arg(long, help = "Path to progress log file")]
    log_file: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    ProgressTuiLogger::initialize(cli.log_file).await?;

    log_key_value_pair(
        "status".to_string(),
        "about to send invalid worker progress".to_string(),
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    log_worker_progress(
        "endpoint-panics".to_string(),
        1.5,
        "this should panic in ProgressScreen::handle_message".to_string(),
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    match ProgressTuiLogger::shutdown().await {
        Ok(()) => {
            println!("unexpected: shutdown succeeded");
            Ok(())
        }
        Err(err) => {
            println!("expected shutdown error after panic: {err}");
            Ok(())
        }
    }
}
