use std::time;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::sleep;
use vstamp::{server, ReplicaConfig};

#[tokio::test]
async fn smoke_replica_server_starts_successfully() {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt()
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

    const DEFAULT_HOST: &str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 4627;
    // Bind a TCP listener
    let default_address = format!("{}:{}", DEFAULT_HOST, DEFAULT_PORT);
    let listener = TcpListener::bind(&default_address)
        .await
        .expect("Failed to bind TCP listener");

    let replica_config = ReplicaConfig {
        listen_address: default_address,
        replicas_addresses: vec![
            "127.0.0.1:4627".to_string(),
            "127.0.0.1:4628".to_string(),
            "127.0.0.1:4629".to_string(),
        ],
    };
    tokio::spawn(server::run(replica_config, listener, signal::ctrl_c()));
    println!("Started replica from test...");
    let ten_millis = time::Duration::from_secs(3);
    sleep(ten_millis).await;
    println!("Bye...");
}
