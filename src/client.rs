use std::io::prelude::*;
use std::net::TcpStream;
use crate::errors::{KvStoreError, Result};

/// TODO: documentation
#[derive(Debug)]
pub enum Command {
    /// TODO: documentation
    Get(String),
    /// TODO: documentation
    Set(String, String),
    /// TODO: documentation
    Remove(String)
}

/// TODO: documentation
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// TODO: documentation
    pub fn new(addr: String) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self {
            stream
        })
    }

    fn serialize(&self, command: Command) -> String {
        match command {
            Command::Get(key) => {
                format!("GET:{}", key)
            },
            Command::Set(key, value) => {
                format!(
                    "SET:{}:{}",
                    key,
                    value,
                )
            },
            Command::Remove(key) => {
                format!("REMOVE:{}", key)
            }
        }
    }

    /// TODO: documentation
    pub fn send(&mut self, command: Command) -> Result<String> {
        let serialized = self.serialize(command);
        self.stream.write_all(serialized.as_bytes())?;
        self.stream.write_all(&b"\n".to_owned())?;
        self.stream.flush()?;

        let mut incoming_string = String::new();
        self.stream.read_to_string(&mut incoming_string)?;

        self.handle_responses(incoming_string)
    }

    /// TODO: documentation
    pub fn handle_responses (&self, incoming: String) -> Result<String> {
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
            Err(KvStoreError::ClientError("Error: Didn't receive any response from server".to_owned()))
        }
    }
}
