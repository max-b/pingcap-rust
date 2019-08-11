extern crate clap;
extern crate kvs;

use std::process;

use kvs::KvStore;

use clap::{App, Arg, SubCommand};

fn main() {
    let matches = App::new("MyApp")
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
        .get_matches();

    if let None = matches.subcommand_name() {
        process::exit(1);
    }

    // let mut store = KvStore::new();

    if let Some(matches) = matches.subcommand_matches("get") {
        eprintln!("unimplemented");
        process::exit(1);
        // store.get(matches.value_of("key").unwrap().to_string());
    }

    if let Some(matches) = matches.subcommand_matches("set") {
        eprintln!("unimplemented");
        process::exit(1);
        // store.set(matches.value_of("key").unwrap().to_string(), matches.value_of("value").unwrap().to_string());
    }

    if let Some(matches) = matches.subcommand_matches("rm") {
        eprintln!("unimplemented");
        process::exit(1);
        // store.remove(matches.value_of("key").unwrap().to_string());
    }
}
