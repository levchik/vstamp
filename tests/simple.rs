use bytes::Bytes;
use once_cell::sync::Lazy;
use std::time;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::debug;
use vstamp::{client, server, KVApp, ReplicaConfig};

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

async fn spawn_servers(n: usize, delay: u64) -> (Vec<JoinHandle<vstamp::Result<()>>>, Vec<String>) {
    Lazy::force(&TRACING);

    let mut replicas_addresses = Vec::new();
    for i in 0..n {
        replicas_addresses.push(format!("127.0.0.1:{}", i + 14621));
    }

    debug!("Starting test servers: {:?}", replicas_addresses);
    let mut servers = Vec::new();
    for i in 0..n {
        let listen_address = replicas_addresses[i].clone();
        let listener = TcpListener::bind(&listen_address)
            .await
            .expect("Failed to bind TCP listener");

        let replica_config = ReplicaConfig {
            listen_address,
            replicas_addresses: replicas_addresses.clone(),
        };
        let app = KVApp::new();
        servers.push(tokio::spawn(server::run(
            app,
            replica_config,
            listener,
            signal::ctrl_c(),
        )));
    }

    debug!("Wait {} seconds for sync...", delay);
    let sleep_duration = time::Duration::from_millis(delay);
    sleep(sleep_duration).await;

    (servers, replicas_addresses)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_saves_app_state_between_client_calls() {
    let (servers, replicas_addresses) = spawn_servers(3, 1).await;
    let addr = replicas_addresses[0].clone();
    let client_id = 777;
    let mut client = client::connect(&addr, client_id).await.unwrap();

    let set_op = Bytes::from_static("S KEY VALUE".as_ref());
    let val = client.request(set_op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 1);
    assert_eq!(val.response, Bytes::from_static("VALUE".as_ref()));

    let get_op = Bytes::from_static("G KEY".as_ref());
    let val = client.request(get_op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 2);
    assert_eq!(val.response, Bytes::from_static("VALUE".as_ref()));

    let delete_op = Bytes::from_static("D KEY".as_ref());
    let val = client.request(delete_op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 3);
    assert_eq!(val.response, Bytes::from_static("".as_ref()));

    let val = client.request(get_op.clone()).await.unwrap();
    assert_eq!(val.view_number, 0);
    assert_eq!(val.request_id, 4);
    assert_eq!(val.response, Bytes::from_static("".as_ref()));

    for s in servers {
        s.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_sends_commit_msgs_in_absence_of_client_requests() {
    let (servers, replicas_addresses) = spawn_servers(3, 100).await;
    let addr = replicas_addresses[0].clone();
    let client_id = 777;
    let mut client = client::connect(&addr, client_id).await.unwrap();

    let delay = 7;
    debug!("Wait {} seconds for sync...", delay);
    let sleep_duration = time::Duration::from_secs(delay);
    sleep(sleep_duration).await;

    for s in servers {
        s.abort();
    }
}
