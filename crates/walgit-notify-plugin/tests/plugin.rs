#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

/// The boundary round-trips: a key published on the host side comes back out
/// of the plugin's own transport, `next` parks without blocking the caller's
/// runtime, and a second load is an independent transport rather than a
/// shared singleton.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a built cdylib; run just test-notify-plugin"]
async fn dynamic_memory_transport_round_trips() {
    let path = std::env::var("WALGIT_TEST_NOTIFY_PLUGIN")
        .expect("just test-notify-plugin sets the cdylib path");
    let path = std::path::Path::new(&path);
    assert!(
        walgit_notify_plugin::load(path, serde_json::json!({"unsupported": true}))
            .await
            .is_err(),
        "the plugin validates its own options"
    );

    let notify = walgit_notify_plugin::load(path, serde_json::json!({}))
        .await
        .unwrap();
    let subscription = notify.clone();
    let subscriber = tokio::spawn(async move { subscription.next().await.unwrap() });

    // Nothing published yet: the subscription is parked, and parking it must
    // not have taken the caller's runtime down with it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!subscriber.is_finished(), "next must wait for a publish");

    // A separate load is a separate transport, so this must not wake it.
    let unrelated = walgit_notify_plugin::load(path, serde_json::json!({}))
        .await
        .unwrap();
    unrelated.publish("other/instance").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !subscriber.is_finished(),
        "one library, two loads, two transports"
    );

    notify.publish("repos/t/r/manifest.pb").await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), subscriber)
        .await
        .expect("the subscription must not hang")
        .unwrap();
    assert_eq!(got.as_deref(), Some("repos/t/r/manifest.pb"));
}

/// A key too large to be an object name is refused before it crosses.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a built cdylib; run just test-notify-plugin"]
async fn oversized_keys_are_refused() {
    let path = std::env::var("WALGIT_TEST_NOTIFY_PLUGIN")
        .expect("just test-notify-plugin sets the cdylib path");
    let notify = walgit_notify_plugin::load(std::path::Path::new(&path), serde_json::json!({}))
        .await
        .unwrap();
    assert!(notify.publish(&"x".repeat(8 * 1024)).await.is_err());
}
