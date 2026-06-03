use std::io;

use clap::Parser;
use research_utility::progress_tui_client;
use research_utility::progress_tui_protocol::progress_screen_server_addr;

#[derive(Debug, Parser)]
#[command(name = "bin_progress_tui")]
#[command(about = "Progress screen TCP client")]
struct Cli {
    #[arg(
        short,
        long,
        default_value_t = progress_screen_server_addr(),
        help = "TCP address for progress server"
    )]
    addr: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    progress_tui_client::run(cli.addr).await
}
