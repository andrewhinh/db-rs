//! db-rs server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `db_rs::server`.
//!
//! The `clap` crate is used for parsing arguments.

use std::path::PathBuf;

use clap::Parser;
use db_rs::{DEFAULT_PORT, server};
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> db_rs::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);
    let replicaof = cli.replicaof.as_ref().and_then(|s| {
        let parts: Vec<&str> = s.splitn(2, &[':', ' '][..]).collect();
        if parts.len() == 2 {
            parts[1]
                .parse()
                .ok()
                .map(|port| (parts[0].to_string(), port))
        } else if parts.len() == 1 && !parts[0].is_empty() {
            Some((parts[0].to_string(), DEFAULT_PORT))
        } else {
            None
        }
    });
    let config = server::ServerConfig {
        aof_path: cli.aof_path,
        snapshot_path: cli.snapshot_path,
        replicaof: replicaof.clone(),
        repl_offset_path: cli.repl_offset_path,
    };

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run_with_config(listener, signal::ctrl_c(), config).await?;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "db-rs-server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,

    #[arg(long)]
    aof_path: Option<PathBuf>,

    #[arg(long)]
    snapshot_path: Option<PathBuf>,

    #[arg(
        long,
        help = "Replicate from leader, e.g. 127.0.0.1:6379 or 127.0.0.1 6379"
    )]
    replicaof: Option<String>,

    #[arg(
        long,
        help = "Path to persist last applied offset (use with replicaof)"
    )]
    repl_offset_path: Option<PathBuf>,
}

fn set_up_logging() -> db_rs::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}
