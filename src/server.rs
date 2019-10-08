use crate::kv::KvsEngine;
use crate::thread_pool::ThreadPool;
use base64;
use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
use slog::{error, info, Logger};
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::marker::Send;
use std::net::{TcpListener, TcpStream};
use std::thread;

/// A struct implementing a key value server with
/// a pluggable db backend
pub struct KvsServer<E: KvsEngine> {
    /// Address the server will listen on
    addr: String,
    /// Pluggable db backend
    store: E,
    /// Logger
    logger: Logger,
    /// A crossbeam channel sender for notifying ourselves when to exit
    sender: Sender<Message>,
    /// A crossbeam channel receiver for knowing when to exit
    receiver: Receiver<Message>,
}

enum Message {
    Terminate,
}

enum ServerResult {
    Ok(String),
    Err(String),
    Exit,
}

fn handle_incoming<E: KvsEngine>(
    store: E,
    stream: TcpStream,
    logger: Logger,
) -> io::Result<(ServerResult, TcpStream)> {
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
        } else if command == "EXIT" {
            ServerResult::Exit
        } else {
            ServerResult::Err("Command not recognized".to_owned())
        }
    } else {
        ServerResult::Err("No command sent".to_owned())
    };

    Ok((store_response, stream))
}

fn handle_response(result: ServerResult, mut stream: TcpStream) -> io::Result<()> {
    match result {
        ServerResult::Ok(response) => {
            stream.write_all(b"OK:")?;
            stream.write_all(base64::encode(response.as_bytes()).as_bytes())?;
        }
        ServerResult::Err(response) => {
            stream.write_all(b"ERR:")?;
            stream.write_all(base64::encode(response.as_bytes()).as_bytes())?;
        }
        _ => {}
    };
    stream.flush()?;
    Ok(())
}

impl<E: KvsEngine> KvsServer<E> {
    /// Create a new key value server listening on an address with
    /// a pluggable storage db backend
    pub fn new(addr: String, store: E, logger: Logger) -> Self {
        let (sender, receiver) = unbounded();
        Self {
            addr,
            store,
            logger,
            sender,
            receiver,
        }
    }

    /// Stop the key value server listening
    pub fn stop(&mut self) {
        self.sender
            .send(Message::Terminate)
            .expect("failed sending message");
    }

    /// Start the key value server listening for connections
    pub fn start<P: ThreadPool + Send + 'static>(
        &mut self,
        thread_pool: P,
    ) -> io::Result<thread::JoinHandle<()>> {
        let store = self.store.clone();
        let logger = self.logger.clone();

        let addr = self.addr.clone();
        let sender = self.sender.clone();
        let receiver = self.receiver.clone();
        let handle = thread::spawn(move || {
            // TODO: error handling for all of these unwraps
            let listener = TcpListener::bind(&addr).unwrap();

            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let store = store.clone();
                let logger = logger.clone();
                let sender = sender.clone();
                let receiver = receiver.clone();

                thread_pool.spawn(move || {
                    // TODO: handle error
                    match handle_incoming(store, stream, logger.clone()) {
                        Err(e) => {
                            error!(logger, "error handling incoming"; "error" => %&e);
                        }
                        Ok((store_response, stream)) => {
                            if let ServerResult::Exit = store_response {
                                sender
                                    .send(Message::Terminate)
                                    .expect("failed sending message");
                            } else {
                                let result = handle_response(store_response, stream);
                                if let Err(e) = result {
                                    error!(logger, "error responding"; "error" => %&e);
                                }
                            }
                        }
                    }
                });
                if let Ok(message) = receiver.try_recv() {
                    if let Message::Terminate = message {
                        break;
                    }
                }
            }
        });
        Ok(handle)
    }
}
