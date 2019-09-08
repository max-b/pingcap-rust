extern crate clap;
extern crate kvs;

use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use std::path::Path;
use std::process;
use base64;

use kvs::KvStore;

use clap::{App, Arg, SubCommand};

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

    let (addr, command) = if let Some(matches) = matches.subcommand_matches("get") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        (addr, format!("GET:{}", matches.value_of("key").unwrap().to_owned()))
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        (addr, format!("SET:{}:{}", matches.value_of("key").unwrap().to_owned(), matches.value_of("value").unwrap().to_owned()))
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        let addr = matches.value_of("addr").unwrap_or(default_addr);
        (addr, format!("REMOVE:{}", matches.value_of("key").unwrap().to_owned()))
    } else {
        (default_addr, String::from("nope"))
    };
    
    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(command.as_bytes())?;
    stream.write_all(&b"\n".to_owned())?;
    stream.flush()?;

    let mut incoming_string = String::new();
    stream.read_to_string(&mut incoming_string)?;

    let mut sections = incoming_string.trim_end().split(":");
    let success_string = sections.next();

    if let Some(success_string) = success_string {
        let response = sections.next()
            .map(|v| String::from_utf8(base64::decode(v).unwrap()).unwrap())
            .unwrap_or("Undefined response from server".to_owned());
        if success_string == "ERR" {
            eprintln!("Error: {}", response);
            process::exit(1);
        } else if success_string == "OK" {
            if response == "NONE" {
                println!("Key not found");
            } else if response.trim() != "" {
                println!("{}", response);
            }
        } else {
            eprintln!("Error: {}", response);
            process::exit(1);
        }
    } else {
        println!("Error: Didn't receive any response from server");
        process::exit(1);
    }
    Ok(())
}
