//! Minimal external-transport example: a queue in this process, proving the
//! boundary round-trips without a broker or a client library.
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
            // Register before re-checking, or a publish landing in between
            // parks this call until the following one.
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
