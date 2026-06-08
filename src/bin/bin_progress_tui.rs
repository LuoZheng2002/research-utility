use std::path::Path;
use std::time::Duration;

use clap::Parser;
use research_utility::progress_tui_reader;

const REFRESH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Parser)]
#[command(name = "bin_progress_tui")]
#[command(about = "Progress screen bincode log reader")]
struct Cli {
    #[arg(short, long, help = "Path to progress log file")]
    log_file: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    if !Path::new(&cli.log_file).is_file() {
        eprintln!(
            "Reader error: progress log file does not exist: {}",
            cli.log_file
        );
        std::process::exit(1);
    }

    if let Err(err) =
        progress_tui_reader::run_with_redraw_interval(cli.log_file, REFRESH_INTERVAL).await
    {
        eprintln!("Reader error: {err}");
        std::process::exit(1);
    }
}
