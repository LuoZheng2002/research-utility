use std::io;
use std::time::Duration;

use clap::Parser;
use research_utility::progress_tui_client;
use research_utility::progress_tui_protocol::progress_screen_server_addr;
use tokio::net::TcpStream;

const REFRESH_INTERVAL: Duration = Duration::from_millis(100);

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    wait_for_server(&cli.addr).await?;
    progress_tui_client::run_with_redraw_interval(cli.addr, REFRESH_INTERVAL).await
}

async fn wait_for_server(addr: &str) -> io::Result<()> {
    let mut waiting_hint_printed = false;

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => {
                if waiting_hint_printed {
                    eprintln!("Server is ready at {addr}. Launching UI...");
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                if !waiting_hint_printed {
                    eprintln!("Server not ready at {addr}. Waiting and retrying every 1s...");
                    waiting_hint_printed = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error),
        }
    }
}
