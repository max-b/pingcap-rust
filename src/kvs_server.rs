use crate::kv::KvsEngine;
use crate::thread_pool::ThreadPool;
use base64;
use slog::{info, error, Logger};
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

/// A struct implementing a key value server with
/// a pluggable db backend
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    /// Address the server will listen on
    addr: String,
    /// Pluggable db backend
    store: E,
    /// A thread pool to run commands
    thread_pool: P,
    /// Logger
    logger: Logger,
}

enum ServerResult {
    Ok(String),
    Err(String),
}

fn handle_incoming<E: KvsEngine>(
    store: E,
    mut stream: TcpStream,
    logger: Logger,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut incoming_string = String::new();

    reader.read_line(&mut incoming_string)?;

    info!(logger, "incoming"; "data" => &incoming_string);

    let mut sections = incoming_string.trim_end().split(':');

    let command = sections.next();
    let store_response = if let Some(command) = command {
        info!(logger, "command"; "command" => &command);
        if command == "GET" {
            let key = sections.next().unwrap();
            info!(logger, "get input"; "key" => &key);
            let result = store.get(key.to_owned());
            result.map_or_else(
                |_err| ServerResult::Err("Error getting value".to_owned()),
                |option| {
                    option.map_or_else(
                        || ServerResult::Ok("NONE".to_owned()),
                        |value| {
                            info!(logger, "get result"; "value" => &value);
                            ServerResult::Ok(value)
                        },
                    )
                },
            )
        } else if command == "SET" {
            let key = sections.next().unwrap();
            let value = sections.next().unwrap();
            info!(logger, "set input"; "key" => &key, "value" => &value);
            let result = store.set(key.to_owned(), value.to_owned());
            result.map_or_else(
                |_err| ServerResult::Err("Error setting key".to_owned()),
                |_| ServerResult::Ok("".to_owned()),
            )
        } else if command == "REMOVE" {
            let key = sections.next().unwrap();
            info!(logger, "remove input"; "key" => &key);
            let result = store.remove(key.to_owned());
            result.map_or_else(
                |_err| ServerResult::Err("Key not found".to_owned()),
                |_| ServerResult::Ok("".to_owned()),
            )
        } else {
            ServerResult::Err("Command not recognized".to_owned())
        }
    } else {
        ServerResult::Err("No command sent".to_owned())
    };

    match store_response {
        ServerResult::Ok(response) => {
            stream.write_all(b"OK:")?;
            stream.write_all(base64::encode(response.as_bytes()).as_bytes())?;
        }
        ServerResult::Err(response) => {
            stream.write_all(b"ERR:")?;
            stream.write_all(base64::encode(response.as_bytes()).as_bytes())?;
        }
    };
    stream.flush()?;
    Ok(())
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a new key value server listening on an address with
    /// a pluggable storage db backend
    pub fn new(addr: String, store: E, thread_pool: P, logger: Logger) -> Self {
        Self {
            addr,
            store,
            thread_pool,
            logger,
        }
    }

    /// TODO: documentation
    /// Start the key value server listening for connections
    pub fn start(&mut self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.addr)?;

        for stream in listener.incoming() {
            let stream = stream?;
            let store = self.store.clone();
            let logger = self.logger.clone();
            self.thread_pool.spawn(move || {
                // TODO: handle error
                if let Err(e) = handle_incoming(store, stream, logger.clone()) {
                    error!(logger, "error handling incoming"; "error" => %&e);
                }
            })
        }
        Ok(())
    }
}
