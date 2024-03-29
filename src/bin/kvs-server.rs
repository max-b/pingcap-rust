extern crate clap;

#[macro_use]
extern crate slog;
extern crate sloggers;

extern crate kvs;

use std::convert::TryInto;
use std::fs;
use std::io;
use std::path::Path;

use clap::{App, Arg};
use num_cpus;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;

use kvs::{KvStore, KvsServer, RayonThreadPool, SharedQueueThreadPool, SledKvsEngine, ThreadPool};

fn get_engine(engine_path: &Path) -> io::Result<Option<String>> {
    match fs::read_to_string(engine_path) {
        Ok(e) => Ok(Some(e)),
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn main() -> io::Result<()> {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "starting up"; "version" => env!("CARGO_PKG_VERSION"));

    let matches = App::new("KvsServer")
        .about("key value store server")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Maxb")
        .arg(
            Arg::with_name("addr")
                .short("a")
                .long("addr")
                .help("address to listen on in IP:PORT format")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("engine")
                .short("e")
                .long("engine")
                .help("key value store engine")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-path")
                .short("p")
                .long("data-path")
                .help("the directory to store data in")
                .takes_value(true),
        )
        .get_matches();

    let data_path = Path::new(matches.value_of("data-path").unwrap_or("./"));
    let addr = matches
        .value_of("addr")
        .unwrap_or("127.0.0.1:4000")
        .to_owned();

    let engine_opt = matches.value_of("engine").unwrap_or("kvs");
    let engine_path = data_path.join("engine");
    let prev_engine = get_engine(&engine_path)?
        .unwrap_or_else(|| engine_opt.to_owned())
        .to_owned();

    info!(logger, "configuration"; "address" => &addr, "engine_opt" => engine_opt, "prev_engine" => &prev_engine, "data_path" => format!("{:?}", &data_path.canonicalize().unwrap()));

    if prev_engine != engine_opt {
        error!(logger, "engine mismatch");
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "engine mismatch".to_owned(),
        ));
    }

    fs::write(&engine_path, engine_opt.as_bytes())?;

    // let thread_pool = RayonThreadPool::new(num_cpus::get().try_into().unwrap()).unwrap();
    let thread_pool = SharedQueueThreadPool::new(num_cpus::get().try_into().unwrap()).unwrap();

    // TODO: better else condition?
    let handle = if engine_opt == "kvs" {
        let store = KvStore::open(data_path).expect("can't open KvStore");
        let mut server = KvsServer::new(addr, store, logger);
        server.start(thread_pool)?
    } else if engine_opt == "sled" {
        let store = SledKvsEngine::open(data_path).expect("can't open sled db");
        let mut server = KvsServer::new(addr, store, logger);
        server.start(thread_pool)?
    } else {
        panic!("server_opt not properly specified");
    };

    handle.join().unwrap();

    Ok(())
}
