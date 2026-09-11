//! Redis/Valkey pub/sub, for deployments that already run one in-cluster.
//!
//! Classic `PUBLISH`/`SUBSCRIBE` over an ordinary connection, deliberately:
//! in Redis Cluster a classic publish crosses the cluster bus to every node,
//! so a publisher and a subscriber attached to different nodes still meet.
//! Sharded pub/sub (`SPUBLISH`) does not, and a cluster client object exposes
//! neither call — so this is the one shape that is correct on a standalone
//! server and a cluster without knowing which it is talking to.
//!
//! Fire-and-forget with no replay. That is the right trade only because the
//! sweep is the correctness mechanism: the cost of a missed message is one
//! sweep interval of latency, so the reconnect below reconciles rather than
//! pretending nothing was lost.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use redis::AsyncCommands;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::{Notify, Wake};

const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    /// `redis://host:6379`, or `rediss://` for TLS.
    url: String,
    #[serde(default = "default_channel")]
    channel: String,
}

fn default_channel() -> String {
    "walgit:object-finalized".into()
}

pub struct RedisNotify {
    client: redis::Client,
    channel: String,
    /// Built on first publish and kept: `ConnectionManager` reconnects
    /// underneath, so the publish path never owns that.
    publisher: Mutex<Option<redis::aio::ConnectionManager>>,
}

impl RedisNotify {
    pub fn new(options: serde_json::Value) -> anyhow::Result<Self> {
        let options: Options = serde_json::from_value(options)?;
        Ok(RedisNotify {
            client: redis::Client::open(options.url)?,
            channel: options.channel,
            publisher: Mutex::new(None),
        })
    }

    async fn publisher(&self) -> anyhow::Result<redis::aio::ConnectionManager> {
        let mut guard = self.publisher.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self.client.get_connection_manager().await?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// One subscription, until the connection ends or fails. `subscribed`
    /// latches so the caller can tell a reconnect from a first attempt.
    async fn receive(&self, on: &Arc<dyn Wake>, subscribed: &mut bool) -> anyhow::Result<()> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(&self.channel).await?;
        tracing::info!(channel = %self.channel, "notify: redis subscribed");
        // Reconcile only once we are listening again: sweeping first would
        // lose anything published between the sweep and the resubscribe.
        if std::mem::replace(subscribed, true) {
            on.reconcile().await;
        }
        let mut stream = pubsub.on_message();
        while let Some(message) = stream.next().await {
            let Ok(key) = message.get_payload::<String>() else {
                tracing::warn!("notify: redis message with a non-string payload");
                continue;
            };
            metrics::counter!("store_notify_received_total", "transport" => "redis").increment(1);
            on.finalized(&key).await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Notify for RedisNotify {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn publish(&self, key: &str) -> anyhow::Result<()> {
        let mut conn = self.publisher().await?;
        let _: () = conn.publish(&self.channel, key).await?;
        Ok(())
    }

    async fn subscribe(&self, on: Arc<dyn Wake>) -> anyhow::Result<()> {
        let mut subscribed = false;
        loop {
            match self.receive(&on, &mut subscribed).await {
                Ok(()) => tracing::warn!("notify: redis subscription closed"),
                Err(e) => tracing::warn!(error = %e, "notify: redis subscription failed"),
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }
}
