use crate::errors::{KvStoreError, Result};
use std::io::prelude::*;
use std::net::TcpStream;

/// A KvsServer command
#[derive(Debug)]
pub enum Command {
    /// KvsServer GET command
    Get(String),
    /// KvsServer SET command
    Set(String, String),
    /// KvsServer REMOVE command
    Remove(String),
    /// KvsServer EXIT command for prompting server to exit
    Exit,
}

/// A Client for sending commands to a KvsServer
#[derive(Debug)]
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Create a new KvsClient
    pub fn new(addr: String) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    fn serialize(&self, command: Command) -> String {
        match command {
            Command::Get(key) => format!("GET:{}", key),
            Command::Set(key, value) => format!("SET:{}:{}", key, value,),
            Command::Remove(key) => format!("REMOVE:{}", key),
            Command::Exit => format!("EXIT"),
        }
    }

    /// Send a command to the KvsServer where the result string is the success response
    /// from the server
    pub fn send(&mut self, command: Command) -> Result<String> {
        let serialized = self.serialize(command);
        self.stream.write_all(serialized.as_bytes())?;
        self.stream.write_all(&b"\n".to_owned())?;
        self.stream.flush()?;

        let mut incoming_string = String::new();
        self.stream.read_to_string(&mut incoming_string)?;

        self.handle_responses(incoming_string)
    }

    fn handle_responses(&self, incoming: String) -> Result<String> {
        let mut sections = incoming.trim_end().split(':');
        let success_string = sections.next();

        if let Some(success_string) = success_string {
            let response = sections
                .next()
                .map(|v| String::from_utf8(base64::decode(v).unwrap()).unwrap())
                .unwrap_or_else(|| "Undefined response from server".to_owned());
            if success_string == "OK" {
                Ok(response)
            } else {
                Err(KvStoreError::ClientError(response))
            }
        } else {
            Err(KvStoreError::ClientError(
                "Error: Didn't receive any response from server".to_owned(),
            ))
        }
    }
}
