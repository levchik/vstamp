use bytes::Bytes;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::time;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::sleep;
use vstamp::{client, server, ReplicaConfig};

static TRACING: Lazy<()> = Lazy::new(|| {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .compact()
        // Display source code file paths
        // .with_file(true)
        // Display source code line numbers
        // .with_line_number(true)
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

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let replica_config = ReplicaConfig {
        listen_address: format!("{}:{}", addr.ip(), addr.port()),
        replicas_addresses: vec![
            format!("{}:{}", addr.ip(), addr.port()),
            "127.0.0.1:4628".to_string(),
            "127.0.0.1:4629".to_string(),
        ],
    };

    tokio::spawn(async move {
        server::run(replica_config, listener, signal::ctrl_c()).await
    });

    addr
}

#[tokio::test]
async fn replica_replies_successfully_to_client_request() {
    let addr = start_server().await;

    let mut client = client::connect(&addr).await.unwrap();
    let op = Bytes::from_static("foo".as_ref());
    let val = client.request(op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 0);
    assert_eq!(val.response, op);
}

#[tokio::test]
async fn replica_replies_with_stored_reply_for_same_client_and_request_id() {
    let addr = start_server().await;

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
    // TODO: check it didn't alter the state of the replica
}
