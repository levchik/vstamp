use bytes::Bytes;
use once_cell::sync::Lazy;
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


// #[tokio::test]
// async fn replica_replies_successfully_to_client_request() {
//     const DEFAULT_HOST: &str = "127.0.0.1";
//     const DEFAULT_PORT: u16 = 4627;
//     // Bind a TCP listener
//     let default_address = format!("{}:{}", DEFAULT_HOST, DEFAULT_PORT);
//     let listener = TcpListener::bind(&default_address)
//         .await
//         .expect("Failed to bind TCP listener");
//
//     let replica_config = ReplicaConfig {
//         listen_address: default_address.clone(),
//         replicas_addresses: vec![
//             "127.0.0.1:4627".to_string(),
//             "127.0.0.1:4628".to_string(),
//             "127.0.0.1:4629".to_string(),
//         ],
//     };
//     let task = tokio::spawn(server::run(replica_config, listener, signal::ctrl_c()));
//     println!("Started replica from test...");
//
//     let mut client = client::connect(&default_address).await.unwrap();
//     let op = Bytes::from_static("foo".as_ref());
//     let val = client.request(op.clone()).await.unwrap();
//     assert_eq!(val.view_number, 0);
//     assert_eq!(val.request_id, 0);
//     assert_eq!(val.response, op);
//     task.abort();
// }

#[tokio::test]
async fn replica_replies_with_stored_reply_for_same_client_and_request_id() {
    const DEFAULT_HOST: &str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 4627;
    // Bind a TCP listener
    let default_address = format!("{}:{}", DEFAULT_HOST, DEFAULT_PORT);
    let listener = TcpListener::bind(&default_address)
        .await
        .expect("Failed to bind TCP listener");

    let replica_config = ReplicaConfig {
        listen_address: default_address.clone(),
        replicas_addresses: vec![
            "127.0.0.1:4627".to_string(),
            "127.0.0.1:4628".to_string(),
            "127.0.0.1:4629".to_string(),
        ],
    };
    tokio::spawn(server::run(replica_config, listener, signal::ctrl_c()));
    println!("Started replica from test...");

    let mut client = client::connect(&default_address).await.unwrap();
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
