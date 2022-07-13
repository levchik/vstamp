use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;
use vstamp::{server, ReplicaConfig};

#[tokio::main]
async fn main() -> vstamp::Result<()> {
    // tracing_subscriber::fmt::try_init()?;
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
    tracing::subscriber::set_global_default(subscriber)?;

    const DEFAULT_HOST: &str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 4627;
    // Bind a TCP listener
    let default_address = format!("{}:{}", DEFAULT_HOST, DEFAULT_PORT);
    let listener = TcpListener::bind(&default_address).await?;

    let replica_config = ReplicaConfig {
        listen_address: default_address,
        replicas_addresses: vec![
            "127.0.0.1:4627".to_string(),
            "127.0.0.1:4628".to_string(),
            "127.0.0.1:4629".to_string(),
        ],
    };
    info!("Starting replica...");
    server::run(replica_config, listener, signal::ctrl_c()).await
}
