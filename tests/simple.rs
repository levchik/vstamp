use bytes::Bytes;
use once_cell::sync::Lazy;
use std::time;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::sleep;
use tracing::debug;
use vstamp::{client, server, ReplicaConfig};

static TRACING: Lazy<()> = Lazy::new(|| {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");
});

async fn spawn_servers(n: usize, delay: u64) -> Vec<String> {
    Lazy::force(&TRACING);

    let mut replicas_addresses = Vec::new();
    for i in 0..n {
        replicas_addresses.push(format!("127.0.0.1:{}", i + 4621));
    }

    debug!("Starting test servers: {:?}", replicas_addresses);

    for i in 0..n {
        let listen_address = replicas_addresses[i].clone();
        let listener = TcpListener::bind(&listen_address)
            .await
            .expect("Failed to bind TCP listener");

        let replica_config = ReplicaConfig {
            listen_address,
            replicas_addresses: replicas_addresses.clone(),
        };

        tokio::spawn(server::run(replica_config, listener, signal::ctrl_c()));
    }

    debug!("Wait {} seconds for sync...", delay);
    let sleep_duration = time::Duration::from_secs(delay);
    sleep(sleep_duration).await;

    replicas_addresses
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_sends_stored_reply_if_latest_request_came_as_duplicate() {
    let replicas_addresses = spawn_servers(3, 1).await;
    let addr = replicas_addresses[0].clone();
    let mut client = client::connect(&addr).await.unwrap();
    let op = Bytes::from_static("foo".as_ref());

    let val = client.request(op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 0);
    assert_eq!(val.response, op);

    let val = client.request(op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 0);
    assert_eq!(val.response, op);
}
