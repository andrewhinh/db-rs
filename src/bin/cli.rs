use std::num::ParseIntError;
use std::str;
use std::time::Duration;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use db_rs::{DEFAULT_PORT, clients::Client};

#[derive(Parser, Debug)]
#[command(name = "db-rs-cli", version, author, about = "Issue Redis commands")]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// Message to ping
        msg: Option<Bytes>,
    },
    /// Get the value of key.
    Get {
        /// Name of key to get
        key: String,
    },
    /// Delete one or more keys.
    Del {
        /// Keys to delete
        keys: Vec<String>,
    },
    /// Return the number of keys that exist.
    Exists {
        /// Keys to check
        keys: Vec<String>,
    },
    /// Set a timeout on key.
    Expire {
        /// Name of key
        key: String,

        /// Timeout in seconds
        seconds: u64,
    },
    /// Return the remaining time to live of a key, in seconds.
    Ttl {
        /// Name of key
        key: String,
    },
    /// Return the remaining time to live of a key, in milliseconds.
    Pttl {
        /// Name of key
        key: String,
    },
    /// Compare-and-set: set key to value only if current equals expected.
    Cas {
        /// Name of key
        key: String,

        /// Expected value; use "null" to expect key missing
        expected: String,

        /// Value to set on success
        value: Bytes,
    },
    /// Set key to hold the string value.
    Set {
        /// Name of key to set
        key: String,

        /// Value to set.
        value: Bytes,

        /// Expire the value after specified amount of time
        #[arg(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    ///  Publisher to send a message to a specific channel.
    Publish {
        /// Name of channel
        channel: String,

        /// Message to publish
        message: Bytes,
    },
    /// Subscribe a client to a specific channel or channels.
    Subscribe {
        /// Specific channel or channels
        channels: Vec<String>,
    },
}

/// Entry point for CLI tool.
///
/// The `[tokio::main]` annotation signals that the Tokio runtime should be
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> db_rs::Result<()> {
    // Enable logging
    tracing_subscriber::fmt::try_init()?;

    // Parse command line arguments
    let cli = Cli::parse();

    // Get the remote address to connect to
    let addr = format!("{}:{}", cli.host, cli.port);

    // Establish a connection
    let mut client = Client::connect(&addr).await?;

    // Process the requested command
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Del { keys } => {
            if keys.is_empty() {
                return Err("key(s) must be provided".into());
            }
            let removed = client.del(&keys).await?;
            println!("{removed}");
        }
        Command::Exists { keys } => {
            if keys.is_empty() {
                return Err("key(s) must be provided".into());
            }
            let count = client.exists(&keys).await?;
            println!("{count}");
        }
        Command::Expire { key, seconds } => {
            let updated = client.expire(&key, seconds).await?;
            println!("{updated}");
        }
        Command::Ttl { key } => {
            let ttl = client.ttl(&key).await?;
            println!("{ttl}");
        }
        Command::Pttl { key } => {
            let pttl = client.pttl(&key).await?;
            println!("{pttl}");
        }
        Command::Cas {
            key,
            expected,
            value,
        } => {
            let exp = if expected.eq_ignore_ascii_case("null") {
                None
            } else {
                Some(Bytes::from(expected.into_bytes()))
            };
            let ok = client.cas(&key, exp, value).await?;
            println!("{}", if ok { 1 } else { 0 });
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            // await messages on channels
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "got message from the channel: {}; message = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
