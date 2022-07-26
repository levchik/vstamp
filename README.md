# vstamp
> A replication technique that handles failures in which nodes crash

**WARNING: This is a work in progress.**

It is a Rust library for building replicated services that use viewstamp replication protocol.
Inspired by [mini-redis](https://github.com/tokio-rs/mini-redis/).

Additionally, I included a Rust server with a simple key-value DB app **(ability to pass your own app is WIP)** to check correctness & demonstrate the use of the library.

I strive to be as much compliant with original paper [Viewstamped Replication Revisited](https://pmg.csail.mit.edu/papers/vr-revisited.pdf) as possible.
This includes naming of structures, commands, and fields.

## Building server

```sh
cargo build --bin vstamp-server
```

## Testing

Since our integration tests run actual servers, they occupy real ports.

They will clush if run in parallel, so you should run them in sequence for now.

```sh
cargo test -- --test-threads=1
```

## Run servers

I've built a simple CLI to run servers with minimal configuration.
So the configuration file is in YAML format, example can be found [here](server-config-example.yaml).

Format of configuration file:

```yaml
replicas_addresses:
  - 127.0.0.1:14621
  - 127.0.0.1:14622
  - 127.0.0.1:14623
```

By default, `vstamp-server` will look for a `server-config.yaml` file in the current directory.
Otherwise, you can specify the path to the configuration file by passing the `--config` flag.
**Note that the configuration file is the same for all servers.**

You must specify which server address you want to occupy by passing the `--index` flag, which is 0-based index of replica address in the configuration file.
    
```sh
cp server-config-example.yaml server-config.yaml
./target/debug/vstamp-server --index 0
./target/debug/vstamp-server --index 1
./target/debug/vstamp-server --index 2
```

## Building CLI

```sh
cargo build --bin vstamp-cli
```

## Using CLI

When you run binary, you'll drop into a REPL. There you can issue commands to kv-store app or (in future releases) manage cluster.

Note that you need a running cluster up and running before you can use the CLI.

```sh
./target/debug/vstamp-cli

127.0.0.1:14621> SET mykey myvalue
myvalue
127.0.0.1:14621> ...
```

You can type help to see available commands and info about how to use them:

```sh
127.0.0.1:14621> help
Available commands:
    PING    Get a response
    QUIT    Quit the REPL
    SET     Set key to a value
    help    Print this message or the help of the given subcommand(s)
    
127.0.0.1:14621> help SET
Set key to a value

USAGE:
    SET <KEY> [VALUE]

ARGS:
    <KEY>      
    <VALUE>    Optional value to assign for this key

OPTIONS:
    -h, --help    Print help information

```

## Roadmap

- [x] TCP-based communication & protocol
- [x] (paper) Normal operation protocol
- [x] Some form of configuration system
- [x] Ability to run servers and send request to them
- [x] Basic tests that show operations are applied correctly to KV store
- [x] Use parking_lot::Mutex as a faster alternative to std::sync::Mutex
- [ ] Ability to run integration tests to them in parallel
- [ ] Design & implement API for checking replicas state
- [ ] Use some crate for Errors (currently they are very clunky)
- [ ] (paper) View change protocol
- [ ] (paper) Recovery protocol
- [ ] (paper) State Transfer protocol
- [ ] (paper) Reconfiguration protocol
- [ ] Client CLI
- [ ] (paper) Client Retries
- [ ] (paper) Client deduplication of responses
- [ ] (paper) Client handling state (e.g. which node is primary)
- [ ] (paper) Client Locating the Group
- [ ] (paper) Client Recovery protocol
- [ ] (paper) Support for non-deterministic operations
- [ ] Support for passing custom app for replication
- [ ] Check correctness & Chaos-based testing (maybe Maelstrom)
- [ ] Use some crate for wire protocol handling instead of manual parsing
- [ ] (paper) Efficient Recovery protocol
- [ ] (paper) Witnesses
- [ ] (paper) Batching
- [ ] (paper) Fast Reads
- [ ] (paper) Shutting down Old Replicas

## Contributing

1. Fork it on GitHub.
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
