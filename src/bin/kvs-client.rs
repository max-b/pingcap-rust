extern crate clap;
extern crate kvs;

use std::io;
use std::process;

use clap::{App, Arg, SubCommand};

use kvs::{KvsClient, Command};

fn main() -> io::Result<()> {
    let addr_arg = Arg::with_name("addr")
        .short("a")
        .long("addr")
        .help("address to connect to in IP:PORT format")
        .takes_value(true);

    let matches = App::new("KvStore")
        .about("key value store")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Maxb")
        .subcommand(
            SubCommand::with_name("get")
                .about("get a key")
                .arg(
                    Arg::with_name("key")
                        .help("the key to fetch")
                        .index(1)
                        .required(true),
                )
                .arg(addr_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("set a key")
                .arg(
                    Arg::with_name("key")
                        .help("the key to set")
                        .index(1)
                        .required(true),
                )
                .arg(
                    Arg::with_name("value")
                        .help("the value to set to")
                        .index(2)
                        .required(true),
                )
                .arg(addr_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove a key")
                .arg(
                    Arg::with_name("key")
                        .help("the key to remove")
                        .index(1)
                        .required(true),
                )
                .arg(addr_arg.clone()),
        )
        .get_matches();

    if matches.subcommand_name().is_none() {
        process::exit(1);
    }

    let default_addr = "127.0.0.1:4000";

    let arg_results = if let Some(matches) = matches.subcommand_matches("get") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        Some((
            addr,
            Command::Get(matches.value_of("key").unwrap().to_owned())
        ))
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        Some((
            addr,
            Command::Set(
                matches.value_of("key").unwrap().to_owned(),
                matches.value_of("value").unwrap().to_owned()
            )
        ))
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        Some((
            addr,
            Command::Remove(matches.value_of("key").unwrap().to_owned())
        ))
    } else {
        None
    };

    match arg_results {
        Some((addr, command)) => {
            let mut client = KvsClient::new(addr.to_owned())?;
            let result = client.send(command);
            match result {
                Err(err) => {
                    eprintln!("Error: {}", err);
                    process::exit(1);
                },
                Ok(response) => {
                    if response == "NONE" {
                        println!("Key not found");
                    } else if response.trim() != "" {
                        println!("{}", response);
                    }
                }
            }
        },
        None => {
            eprintln!("Command invalid: {:?}", matches);
            process::exit(1);
        }
    }
    Ok(())
}
