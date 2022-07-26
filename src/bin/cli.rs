use bytes::Bytes;
use std::borrow::BorrowMut;
use std::io::Write;

use clap::{arg, Command};
use vstamp::client;
use vstamp::client::Client;

#[tokio::main]
async fn main() -> Result<(), String> {
    let addr = "127.0.0.1:14621";
    let client_id = 666; // just random id
    let mut client = client::connect(&addr, client_id).await;
    if client.is_err() {
        write!(std::io::stdout(), "{}\r\n", client.unwrap_err().to_string())
            .map_err(|e| e.to_string())?;
        std::io::stdout().flush().map_err(|e| e.to_string())?;
        return Err("Failed to connect to server".to_string());
    }

    loop {
        let line = readline()?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match respond(line, client.as_mut().unwrap()).await {
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
    let matches = cli()
        .try_get_matches_from(&args)
        .map_err(|e| e.to_string())?;
    match matches.subcommand() {
        Some(("PING", _matches)) => {
            write!(std::io::stdout(), "PONG\r\n")
                .map_err(|e| e.to_string())?;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
        }
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
        Some((name, _matches)) => unimplemented!("{}", name),
        None => unreachable!("subcommand required"),
    }

    Ok(false)
}

fn cli() -> Command<'static> {
    // strip out usage
    const PARSER_TEMPLATE: &str = "\
        {all-args}
    ";
    // strip out name/version
    const APPLET_TEMPLATE: &str = "\
        {about-with-newline}\n\
        {usage-heading}\n    {usage}\n\
        \n\
        {all-args}{after-help}\
    ";

    Command::new("repl")
        .multicall(true)
        .arg_required_else_help(true)
        .subcommand_required(true)
        .subcommand_value_name("Command")
        .subcommand_help_heading("Available commands")
        .help_template(PARSER_TEMPLATE)
        .subcommand(
            Command::new("PING")
                .about("Get a response")
                .help_template(APPLET_TEMPLATE),
        )
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
}

fn readline() -> Result<String, String> {
    write!(std::io::stdout(), "127.0.0.1:14621> ")
        .map_err(|e| e.to_string())?;
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut buffer = String::new();
    std::io::stdin()
        .read_line(&mut buffer)
        .map_err(|e| e.to_string())?;
    Ok(buffer)
}
