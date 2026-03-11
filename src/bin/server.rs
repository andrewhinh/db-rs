//! db-rs server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `db_rs::server`.
//!
//! The `clap` crate is used for parsing arguments.

use clap::Parser;
use db_rs::{DEFAULT_PORT, server};
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> db_rs::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "db-rs-server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

fn set_up_logging() -> db_rs::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}
