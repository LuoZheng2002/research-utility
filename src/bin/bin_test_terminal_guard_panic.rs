use std::io;
use std::time::Duration;

use research_utility::progress_tui_server::{
    ProgressTuiServer, log_key_value_pair, log_worker_progress,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    ProgressTuiServer::initialize(None, |_command| {}).await?;

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

    match ProgressTuiServer::shutdown().await {
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
