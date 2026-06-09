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
    #[arg(long, help = "Optional shell script run before Space-key refresh")]
    sync_script_path: Option<String>,
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
    if let Some(sync_script_path) = &cli.sync_script_path {
        if !Path::new(sync_script_path).is_file() {
            eprintln!(
                "Reader error: sync script file does not exist: {}",
                sync_script_path
            );
            std::process::exit(1);
        }
    }

    if let Err(err) = progress_tui_reader::run_with_redraw_interval_and_sync_script(
        cli.log_file,
        REFRESH_INTERVAL,
        cli.sync_script_path,
    )
    .await
    {
        eprintln!("Reader error: {err}");
        std::process::exit(1);
    }
}
