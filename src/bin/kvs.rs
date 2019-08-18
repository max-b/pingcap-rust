extern crate clap;
extern crate kvs;

use std::io;
use std::io::prelude::*;
use std::process;
use std::path::Path;

use kvs::KvStore;

use clap::{App, Arg, SubCommand};

fn main() -> io::Result<()> {
    let matches = App::new("KvStore")
        .about("key value store")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Maxb")
        .subcommand(
            SubCommand::with_name("get").about("get a key").arg(
                Arg::with_name("key")
                    .help("the key to fetch")
                    .index(1)
                    .required(true),
            ),
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
                ),
        )
        .subcommand(
            SubCommand::with_name("rm").about("remove a key").arg(
                Arg::with_name("key")
                    .help("the key to remove")
                    .index(1)
                    .required(true),
            ),
        )
        .arg(
            Arg::with_name("data-path")
                .short("p")
                .long("data-path")
                .help("the directory to store data in")
                .takes_value(true)
        )
        .get_matches();

    let file_path = matches.value_of("data-path").unwrap_or("./");
    if matches.subcommand_name().is_none() {
        process::exit(1);
    }

    let mut store = KvStore::open(Path::new(file_path)).expect("can't open KvStore");

    if let Some(matches) = matches.subcommand_matches("get") {
        match store.get(matches.value_of("key").unwrap().to_owned()).map_err(|err| io::Error::new(io::ErrorKind::Other, err))? {
            Some(val) => {
                println!("{}", val);
            },
            None => {
                println!("Key not found");
            },
        }
    }

    if let Some(matches) = matches.subcommand_matches("set") {
        store.set(matches.value_of("key").unwrap().to_owned(), matches.value_of("value").unwrap().to_owned()).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;;
    }

    if let Some(matches) = matches.subcommand_matches("rm") {
        match store.remove(matches.value_of("key").unwrap().to_owned()) {
            Ok(_) => {},
            Err(_) => {
                println!("Key not found");
                process::exit(1);
            }
        }
    }

    Ok(())
}
