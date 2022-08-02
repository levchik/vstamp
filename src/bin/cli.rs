use bytes::Bytes;
use std::borrow::BorrowMut;
use std::io::Write;

use clap::{arg, value_parser, Command};
use vstamp::client;
use vstamp::client::Client;

#[tokio::main]
async fn main() -> Result<(), String> {
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: &str = "14621";

    let mut connection_host = DEFAULT_HOST.to_string();
    let mut connection_port = DEFAULT_PORT.to_string();
    let mut client_id: u128 = 666;

    let matches = cli().get_matches();

    if let Some(host) = matches.get_one::<String>("host") {
        connection_host = host.clone();
    }
    if let Some(port) = matches.get_one::<String>("port") {
        connection_port = port.clone();
    }
    let addr = format!("{}:{}", connection_host, connection_port);

    if let Some(id) = matches.get_one::<u128>("id") {
        client_id = *id;
    }

    let mut client = client::connect(&addr, client_id).await;
    if client.is_err() {
        write!(std::io::stdout(), "{}\r\n", client.unwrap_err().to_string())
            .map_err(|e| e.to_string())?;
        std::io::stdout().flush().map_err(|e| e.to_string())?;
        return Err("Failed to connect to server".to_string());
    }

    loop {
        let line = readline(&addr)?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match respond(line, client.as_mut().unwrap()).await {
            // match respond(line).await {
            Ok(quit) => {
                if quit {
                    break;
                }
            }
            Err(err) => {
                write!(std::io::stdout(), "{}\r\n", err)
                    .map_err(|e| e.to_string())?;
                std::io::stdout().flush().map_err(|e| e.to_string())?;
            }
        }
    }

    Ok(())
}

async fn respond(line: &str, client: &mut Client) -> Result<bool, String> {
    let args = shlex::split(line).ok_or("error: Invalid quoting")?;
    let matches = repl()
        .try_get_matches_from(&args)
        .map_err(|e| e.to_string())?;

    match matches.subcommand() {
        Some(("QUIT", _matches)) => {
            write!(std::io::stdout(), "Bye...\r\n")
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
            return Ok(true);
        }
        Some(("SET", m)) => {
            let k = m
                .value_of("KEY")
                .ok_or("error: Missing key\r\n")?
                .to_string();
            let v = match m.value_of("VALUE") {
                None => Bytes::new(),
                Some(v) => Bytes::from(v.to_string()),
            };
            // TODO: handle connection reset by server
            let reply = client.set(Bytes::from(k), v).await.unwrap();
            let response = std::str::from_utf8(&*reply.response).unwrap();
            write!(std::io::stdout(), "{}\r\n", response)
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
        }
        Some(("GET", m)) => {
            let k = m
                .value_of("KEY")
                .ok_or("error: Missing key\r\n")?
                .to_string();
            // TODO: handle connection reset by server
            let reply = client.get(Bytes::from(k)).await.unwrap();
            let response = std::str::from_utf8(&*reply.response).unwrap();
            write!(std::io::stdout(), "{}\r\n", response)
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
        }
        Some(("DEL", m)) => {
            let k = m
                .value_of("KEY")
                .ok_or("error: Missing key\r\n")?
                .to_string();
            // TODO: handle connection reset by server
            let reply = client.delete(Bytes::from(k)).await.unwrap();
            let response = std::str::from_utf8(&*reply.response).unwrap();
            write!(std::io::stdout(), "{}\r\n", response)
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
        }
        Some(("SIZE", m)) => {
            // TODO: handle connection reset by server
            let reply = client.get_size().await.unwrap();
            let response = std::str::from_utf8(&*reply.response).unwrap();
            write!(std::io::stdout(), "{}\r\n", response)
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
        }
        Some((name, _matches)) => unimplemented!("{}", name),
        None => unreachable!("subcommand required"),
    }

    Ok(false)
}

fn cli() -> Command<'static> {
    Command::new("vstamp")
        .about("A CLI for interacting with cluster of vstamp nodes.")
        .subcommand_required(false)
        .arg_required_else_help(false)
        .arg(
            arg!(
                -h --host <HOST> "Sets a custom host to connect to"
            )
            .required(false),
        )
        .arg(
            arg!(
                -p --port <PORT> "Sets a custom port to connect to"
            )
            .required(false),
        )
        .arg(
            arg!(
                --id <ID> "Sets a custom id, used for identifying the client"
            )
            .required(false)
            .value_parser(value_parser!(u128)),
        )
}

fn repl() -> Command<'static> {
    const PARSER_TEMPLATE: &str = "\
        {all-args}
    ";
    const APPLET_TEMPLATE: &str = "\
        {about-with-newline}\n\
        {usage-heading}\n    {usage}\n\
        \n\
        {all-args}{after-help}\
    ";
    Command::new("REPL")
        .multicall(true)
        .arg_required_else_help(true)
        .subcommand_required(true)
        .subcommand_value_name("Command")
        .subcommand_help_heading("Available commands")
        .subcommand(
            Command::new("QUIT")
                .alias("exit")
                .about("Quit the REPL")
                .help_template(APPLET_TEMPLATE),
        )
        .subcommand(
            Command::new("SET")
                .arg(arg!(<KEY>))
                .arg(arg!([VALUE] "Optional value to assign for this key"))
                .alias("s")
                .about("Set key to a value")
                .help_template(APPLET_TEMPLATE),
        )
        .subcommand(
            Command::new("GET")
                .arg(arg!(<KEY>))
                .alias("g")
                .about("Get key's value")
                .help_template(APPLET_TEMPLATE),
        )
        .subcommand(
            Command::new("DEL")
                .arg(arg!(<KEY>))
                .alias("d")
                .about("Delete key")
                .help_template(APPLET_TEMPLATE),
        )
        .subcommand(
            Command::new("SIZE")
                .alias("z")
                .about("Get current size of a database")
                .help_template(APPLET_TEMPLATE),
        )
}

fn readline(addr: &String) -> Result<String, String> {
    write!(std::io::stdout(), "{}> ", addr).map_err(|e| e.to_string())?;
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut buffer = String::new();
    std::io::stdin()
        .read_line(&mut buffer)
        .map_err(|e| e.to_string())?;
    Ok(buffer)
}
