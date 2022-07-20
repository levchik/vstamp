use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_yaml;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;
use vstamp::{server, KVApp, ReplicaConfig};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[clap(short, long, value_parser, default_value = "server-config.yaml")]
    config: String,

    /// Index of the server in the configuration file
    #[clap(short, long, value_parser)]
    index: u8,
}

async fn run_server(
    replicas_addresses: Vec<String>,
    index: u8,
) -> vstamp::Result<()> {
    let replica_config = ReplicaConfig {
        listen_address: replicas_addresses[index as usize].clone(),
        replicas_addresses: replicas_addresses.clone(),
    };

    // Bind a TCP listener
    let listener = TcpListener::bind(&replica_config.listen_address).await?;

    info!("Creating app...");
    let app = KVApp::new();

    info!("Starting replica...");
    server::run(app, replica_config, listener, signal::ctrl_c()).await
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ServerConfig {
    replicas_addresses: Vec<String>,
}

#[tokio::main]
async fn main() -> vstamp::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let index = args.index;
    let f = std::fs::File::open(args.config)?;
    let server_config: ServerConfig = serde_yaml::from_reader(f)?;
    info!("
            ┌────────────────────────────────────────────────────────────────────────────┐
            │                                                                            │
            │                                                                            │
            │                                                                            │
            │      **      **  ******** **********     **     ****     **** *******      │
            │     /**     /** **////// /////**///     ****   /**/**   **/**/**////**     │
            │     /**     /**/**           /**       **//**  /**//** ** /**/**   /**     │
            │     //**    ** /*********    /**      **  //** /** //***  /**/*******      │
            │      //**  **  ////////**    /**     **********/**  //*   /**/**////       │
            │       //****          /**    /**    /**//////**/**   /    /**/**           │
            │        //**     ********     /**    /**     /**/**        /**/**           │
            │         //     ////////      //     //      // //         // //            │
            │                                                                            │
            │                                                                            │
            │                                                                            │
            │                                                                            │
            └────────────────────────────────────────────────────────────────────────────┘");
    info!(
        "Server will be run under the following config:\r\n\t{:?}",
        server_config
    );
    run_server(server_config.replicas_addresses, index).await
}
