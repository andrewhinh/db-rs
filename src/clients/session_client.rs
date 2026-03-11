//! Session-consistent client for read-your-writes and monotonic reads.
//!
//! Tracks `last_write_offset` and `last_read_offset` so reads from followers
//! see at least the client's own writes and never go backward in time.

use bytes::Bytes;

use crate::clients::Client;

pub struct SessionClient {
    client: Client,
    last_write_offset: i64,
    last_read_offset: i64,
}

impl SessionClient {
    pub fn new(client: Client) -> SessionClient {
        SessionClient {
            client,
            last_write_offset: -1,
            last_read_offset: -1,
        }
    }

    fn min_offset(&self) -> i64 {
        self.last_write_offset.max(self.last_read_offset)
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.client.set(key, value).await?;
        self.last_write_offset = self.client.offset().await?;
        Ok(())
    }

    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: std::time::Duration,
    ) -> crate::Result<()> {
        self.client.set_expires(key, value, expiration).await?;
        self.last_write_offset = self.client.offset().await?;
        Ok(())
    }

    pub async fn del(&mut self, keys: &[String]) -> crate::Result<u64> {
        let n = self.client.del(keys).await?;
        if n > 0 {
            self.last_write_offset = self.client.offset().await?;
        }
        Ok(n)
    }

    pub async fn expire(&mut self, key: &str, seconds: u64) -> crate::Result<u64> {
        let n = self.client.expire(key, seconds).await?;
        if n > 0 {
            self.last_write_offset = self.client.offset().await?;
        }
        Ok(n)
    }

    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        let min = self.min_offset();
        let min_offset = if min >= 0 { Some(min) } else { None };
        let v = self.client.get_with_offset(key, min_offset).await?;
        if min >= 0 {
            self.last_read_offset = min;
        }
        Ok(v)
    }

    pub async fn offset(&mut self) -> crate::Result<i64> {
        self.client.offset().await
    }
}
