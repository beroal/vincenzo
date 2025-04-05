use clap::Parser;
use tokio::{runtime::Runtime, spawn, sync::mpsc};
use tracing::{warn, debug, Level};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt::time::OffsetTime, FmtSubscriber};
use vincenzo::{
    config::Config, daemon::{Args, Daemon}, error::Error
};

use vcz_ui::{UIMsg, UI};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let tmp = std::env::temp_dir();
    let time = std::time::SystemTime::now();
    let timestamp =
        time.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();

    let file_appender = RollingFileAppender::new(
        Rotation::NEVER,
        tmp,
        format!("vcz-{timestamp}.log"),
    );
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .with_writer(non_blocking)
        .with_timer(OffsetTime::new(
            time::UtcOffset::current_local_offset()
                .unwrap_or(time::UtcOffset::UTC),
            time::format_description::parse(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]",
            )
            .unwrap(),
        ))
        .with_ansi(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let args = Args::parse();
    let config = Config::load().await.unwrap();

    let download_dir = args.download_dir.unwrap_or(config.download_dir.clone());
    let daemon_addr = args.daemon_addr.unwrap_or(
        config.daemon_addr.unwrap_or("127.0.0.1:3030".parse().unwrap()),
    );

    let mut daemon = Daemon::new(download_dir);
    daemon.config.listen = daemon_addr;
    daemon.config.no_tracker = args.no_tracker;

    let disk_tx = daemon.get_disk_tx();

    let rt = Runtime::new().unwrap();
    let handle = std::thread::spawn(move || {
        rt.block_on(async {
            daemon.run().await.unwrap();
            debug!("daemon exited run");
        });
    });

    let http_server_addr = args.http_server_addr.or(
        config.http_server_addr
    );
    if let Some(http_server_addr) = http_server_addr {
        spawn(async move {
            match tokio::net::TcpListener::bind(http_server_addr).await {
                Err(error) => warn!(?error, "when binding an HTTP socket"),
                Ok(tcp_listener) => vcz_http_server::main(
                    tcp_listener,
                    disk_tx,
                    std::future::pending::<()>(),
                    std::time::Duration::from_secs(2),
                ).await,
            }
        });
    }

    // Start and run the terminal UI
    let (fr_tx, fr_rx) = mpsc::channel::<UIMsg>(300);
    let mut fr = UI::new(fr_tx.clone());

    spawn(async move {
        fr.run(fr_rx, daemon_addr).await.unwrap();
        debug!("ui exited run");
    });

    let args = Args::parse();

    // If the user passed a magnet through the CLI,
    // start this torrent immediately
    if let Some(magnet) = args.magnet {
        fr_tx.send(UIMsg::NewTorrent(magnet)).await.unwrap();
    }

    handle.join().unwrap();

    Ok(())
}
