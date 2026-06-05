use std::io;
use std::time::Duration;

use clap::Parser;
use research_utility::progress_tui_protocol::DEFAULT_PROGRESS_SCREEN_TCP_PORT;
use research_utility::progress_tui_server::{
    ProgressTuiServer, log_key_value_pair, log_worker_progress,
};

#[derive(Debug, Parser)]
#[command(name = "bin_test_terminal_guard_panic")]
#[command(about = "Triggers a panic path in progress TUI handling")]
struct Cli {
    #[arg(
        long,
        default_value_t = DEFAULT_PROGRESS_SCREEN_TCP_PORT,
        help = "TCP port for progress TUI server"
    )]
    tui_server_port: u16,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    ProgressTuiServer::initialize(cli.tui_server_port, None, |_command| {}).await?;

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
