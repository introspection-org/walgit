//! Minimal external-transport example: a queue in this process, with no broker
//! and no client library. It exists to prove the boundary round-trips, which
//! is as far as an in-tree transport should go — a real one (GCP Pub/Sub,
//! Redis pub/sub, NATS) carries credentials and routing walgit should not.
use std::collections::VecDeque;

use tokio::sync::{Mutex, Notify};
use walgit_notify_plugin::NotifySource;

#[derive(Default)]
struct MemoryNotify {
    queue: Mutex<VecDeque<String>>,
    waiting: Notify,
}

#[async_trait::async_trait]
impl NotifySource for MemoryNotify {
    async fn publish(&self, key: &str) -> anyhow::Result<()> {
        self.queue.lock().await.push_back(key.to_owned());
        self.waiting.notify_one();
        Ok(())
    }

    async fn next(&self) -> anyhow::Result<Option<String>> {
        loop {
            // Register before re-checking: a publish landing between the check
            // and the await would otherwise park this call until the next one.
            let pending = self.waiting.notified();
            if let Some(key) = self.queue.lock().await.pop_front() {
                return Ok(Some(key));
            }
            pending.await;
        }
    }
}

async fn create(config: serde_json::Value) -> anyhow::Result<MemoryNotify> {
    anyhow::ensure!(
        config.as_object().is_some_and(serde_json::Map::is_empty),
        "the memory transport takes no options"
    );
    Ok(MemoryNotify::default())
}

walgit_notify_plugin::export_notify_plugin!(create);
