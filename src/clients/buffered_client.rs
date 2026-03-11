use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;

use crate::Result;
use crate::clients::Client;

// Enum used to message pass the requested command from the `BufferedClient`
// handle
#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
    Del(Vec<String>),
    Exists(Vec<String>),
    Expire(String, u64),
    Ttl(String),
    Pttl(String),
}

#[derive(Debug)]
enum Response {
    Value(Option<Bytes>),
    Unit,
    Count(u64),
    Signed(i64),
}

// Message type sent over the channel to the connection task.
//
// `Command` is the command to forward to the connection.
//
// `oneshot::Sender` is a channel type that sends a **single** value. It is used
// here to send the response received from the connection back to the original
// requester.
type Message = (Command, oneshot::Sender<Result<Response>>);

/// Receive commands sent through the channel and forward them to client. The
/// response is returned back to the caller via a `oneshot`.
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    // Repeatedly pop messages from the channel. A return value of `None`
    // indicates that all `BufferedClient` handles have dropped and there will never
    // be another message sent on the channel.
    while let Some((cmd, tx)) = rx.recv().await {
        // The command is forwarded to the connection
        let response = match cmd {
            Command::Get(key) => client.get(&key).await.map(Response::Value),
            Command::Set(key, value) => client.set(&key, value).await.map(|_| Response::Unit),
            Command::Del(keys) => client.del(&keys).await.map(Response::Count),
            Command::Exists(keys) => client.exists(&keys).await.map(Response::Count),
            Command::Expire(key, seconds) => {
                client.expire(&key, seconds).await.map(Response::Count)
            }
            Command::Ttl(key) => client.ttl(&key).await.map(Response::Signed),
            Command::Pttl(key) => client.pttl(&key).await.map(Response::Signed),
        };

        // Send the response back to the caller.
        //
        // Failing to send the message indicates the `rx` half dropped
        // before receiving the message. This is a normal runtime event.
        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// Create a new client request buffer
    ///
    /// The `Client` performs Redis commands directly on the TCP connection.
    /// Only a single request may be in-flight at a given time and
    /// operations require mutable access to the `Client` handle. This
    /// prevents using a single Redis connection from multiple Tokio tasks.
    ///
    /// The strategy for dealing with this class of problem is to spawn a
    /// dedicated Tokio task to manage the Redis connection and using
    /// "message passing" to operate on the connection. Commands are pushed
    /// into a channel. The connection task pops commands off of the channel
    /// and applies them to the Redis connection. When the response is
    /// received, it is forwarded to the original requester.
    ///
    /// The returned `BufferedClient` handle may be cloned before passing the
    /// new handle to separate tasks.
    pub fn buffer(client: Client) -> BufferedClient {
        // Setting the message limit to a hard coded value of 32. in a real-app, the
        // buffer size should be configurable, but we don't need to do that here.
        let (tx, rx) = channel(32);

        // Spawn a task to process requests for the connection.
        tokio::spawn(async move { run(client, rx).await });

        // Return the `BufferedClient` handle.
        BufferedClient { tx }
    }

    /// Get the value of a key.
    ///
    /// Same as `Client::get` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        // Initialize a new `Get` command to send via the channel.
        let get = Command::Get(key.into());

        // Initialize a new oneshot to be used to receive the response back from the
        // connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((get, tx)).await?;

        // Await the response
        match rx.await {
            Ok(res) => match res? {
                Response::Value(value) => Ok(value),
                _ => Err("unexpected response for GET".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Set `key` to hold the given `value`.
    ///
    /// Same as `Client::set` but requests are **buffered** until the associated
    /// connection has the ability to send the request
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        // Initialize a new `Set` command to send via the channel.
        let set = Command::Set(key.into(), value);

        // Initialize a new oneshot to be used to receive the response back from the
        // connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((set, tx)).await?;

        // Await the response
        match rx.await {
            Ok(res) => match res? {
                Response::Unit => Ok(()),
                _ => Err("unexpected response for SET".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Delete one or more keys.
    ///
    /// Requests are buffered until the associated
    /// connection has the ability to send the request.
    pub async fn del(&mut self, keys: &[String]) -> Result<u64> {
        let del = Command::Del(keys.to_vec());
        let (tx, rx) = oneshot::channel();

        self.tx.send((del, tx)).await?;

        match rx.await {
            Ok(res) => match res? {
                Response::Count(count) => Ok(count),
                _ => Err("unexpected response for DEL".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Return the number of keys that exist.
    ///
    /// Requests are buffered until the
    /// associated connection has the ability to send the request.
    pub async fn exists(&mut self, keys: &[String]) -> Result<u64> {
        let exists = Command::Exists(keys.to_vec());
        let (tx, rx) = oneshot::channel();

        self.tx.send((exists, tx)).await?;

        match rx.await {
            Ok(res) => match res? {
                Response::Count(count) => Ok(count),
                _ => Err("unexpected response for EXISTS".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Set a timeout on key.
    ///
    /// Requests are buffered until the associated connection can send.
    pub async fn expire(&mut self, key: &str, seconds: u64) -> Result<u64> {
        let expire = Command::Expire(key.into(), seconds);
        let (tx, rx) = oneshot::channel();

        self.tx.send((expire, tx)).await?;

        match rx.await {
            Ok(res) => match res? {
                Response::Count(count) => Ok(count),
                _ => Err("unexpected response for EXPIRE".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Return the remaining time to live of a key, in seconds.
    ///
    /// Requests are buffered until the associated connection can send.
    pub async fn ttl(&mut self, key: &str) -> Result<i64> {
        let ttl = Command::Ttl(key.into());
        let (tx, rx) = oneshot::channel();

        self.tx.send((ttl, tx)).await?;

        match rx.await {
            Ok(res) => match res? {
                Response::Signed(value) => Ok(value),
                _ => Err("unexpected response for TTL".into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// Return the remaining time to live of a key, in milliseconds.
    ///
    /// Requests are buffered until the associated connection can send.
    pub async fn pttl(&mut self, key: &str) -> Result<i64> {
        let pttl = Command::Pttl(key.into());
        let (tx, rx) = oneshot::channel();

        self.tx.send((pttl, tx)).await?;

        match rx.await {
            Ok(res) => match res? {
                Response::Signed(value) => Ok(value),
                _ => Err("unexpected response for PTTL".into()),
            },
            Err(err) => Err(err.into()),
        }
    }
}
