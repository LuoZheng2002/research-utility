use std::io;
use std::time::Duration;

use clap::Parser;
use research_utility::progress_tui_reader;

const REFRESH_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_LOG_FILE_PATH: &str = "progress_tui_log.bin";

#[derive(Debug, Parser)]
#[command(name = "bin_progress_tui")]
#[command(about = "Progress screen bincode log reader")]
struct Cli {
    #[arg(short, long, default_value = DEFAULT_LOG_FILE_PATH, help = "Path to progress log file")]
    log_file: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    progress_tui_reader::run_with_redraw_interval(cli.log_file, REFRESH_INTERVAL).await
}
