use crate::kv::KvsEngine;
use base64;
use slog::{info, Logger};
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

/// A struct implementing a key value server with
/// a pluggable db backend
pub struct KvsServer {
    /// Address the server will listen on
    addr: String,
    /// Pluggable db backend
    store: Box<dyn KvsEngine>,
    /// Logger
    logger: Logger,
}

enum ServerResult {
    Ok(String),
    Err(String),
}

impl KvsServer {
    /// Create a new key value server listening on an address with
    /// a pluggable storage db backend
    pub fn new(addr: String, store: Box<dyn KvsEngine>, logger: Logger) -> Self {
        Self {
            addr,
            store,
            logger,
        }
    }

    /// Start the key value server listening for connections
    pub fn start(&mut self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.addr)?;

        for stream in listener.incoming() {
            let mut stream = stream?;
            let mut reader = BufReader::new(stream.try_clone()?);
            let mut incoming_string = String::new();

            reader.read_line(&mut incoming_string)?;

            info!(self.logger, "incoming"; "data" => &incoming_string);

            let mut sections = incoming_string.trim_end().split(':');

            let command = sections.next();
            let store_response = if let Some(command) = command {
                info!(self.logger, "command"; "command" => &command);
                if command == "GET" {
                    let key = sections.next().unwrap();
                    info!(self.logger, "get input"; "key" => &key);
                    let result = self.store.get(key.to_owned());
                    result.map_or_else(
                        |_err| ServerResult::Err("Error getting value".to_owned()),
                        |option| {
                            option.map_or_else(
                                || ServerResult::Ok("NONE".to_owned()),
                                |value| {
                                    info!(self.logger, "get result"; "value" => &value);
                                    ServerResult::Ok(value)
                                },
                            )
                        },
                    )
                } else if command == "SET" {
                    let key = sections.next().unwrap();
                    let value = sections.next().unwrap();
                    info!(self.logger, "set input"; "key" => &key, "value" => &value);
                    let result = self.store.set(key.to_owned(), value.to_owned());
                    result.map_or_else(
                        |_err| ServerResult::Err("Error setting key".to_owned()),
                        |_| ServerResult::Ok("".to_owned()),
                    )
                } else if command == "REMOVE" {
                    let key = sections.next().unwrap();
                    info!(self.logger, "remove input"; "key" => &key);
                    let result = self.store.remove(key.to_owned());
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
        }
        Ok(())
    }
}
