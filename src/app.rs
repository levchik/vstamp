use std::borrow::Borrow;
use bytes::Bytes;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

struct SetCmd {
    key: Bytes,
    value: Bytes,
    timestamp: u128,
}

struct GetCmd {
    key: Bytes,
}

struct DeleteCmd {
    key: Bytes,
    timestamp: u128,
}

#[derive(Debug)]
pub struct KVEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub(crate) type GuardedKVApp = Arc<Mutex<KVApp>>;

#[derive(Debug)]
pub struct KVApp {
    data: Vec<KVEntry>,
    size: usize,
}

impl KVApp {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            size: 0,
        }
    }

    pub fn apply(&mut self, mut request: Bytes) -> Bytes {
        let request_str = std::str::from_utf8(&*request).unwrap().to_string();
        let parts: Vec<String> = request_str.split_whitespace().map(String::from).collect();

        let cmd = parts[0].as_str();
        match cmd {
            "S" => {
                let cmd = self.parse_set(parts);
                self.set(&*cmd.key, &*cmd.value, cmd.timestamp);
                Bytes::from(cmd.value)
            }
            "G" => {
                let cmd = self.parse_get(parts);
                match self.get(&*cmd.key) {
                    Some(entry) => match entry.value.as_ref() {
                        Some(value) => Bytes::from(value.clone()),
                        None => Bytes::from(""),
                    },
                    None => Bytes::from_static("".as_ref()),
                }
            }
            "D" => {
                let cmd = self.parse_delete(parts);
                self.delete(&*cmd.key, cmd.timestamp);
                Bytes::from_static("".as_ref())
            }
            _ => unimplemented!()
        }
    }

    fn parse_set(&mut self, tokens: Vec<String>) -> SetCmd {
        let key = tokens[1].clone();
        let value = tokens[2].clone();
        let timestamp = tokens[3].clone();
        SetCmd {
            key: Bytes::from(key),
            value: Bytes::from(value),
            timestamp: timestamp.parse::<u128>().unwrap(),
        }
    }

    fn parse_get(&mut self, tokens: Vec<String>) -> GetCmd {
        let key = tokens[1].clone();
        GetCmd {
            key: Bytes::from(key),
        }
    }

    fn parse_delete(&mut self, tokens: Vec<String>) -> DeleteCmd {
        let key = tokens[1].clone();
        let timestamp = tokens[2].clone();
        DeleteCmd {
            key: Bytes::from(key),
            timestamp: timestamp.parse::<u128>().unwrap(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &[KVEntry] {
        &self.data
    }

    pub fn size(&self) -> usize {
        self.size
    }

    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.data.binary_search_by_key(&key, |e| e.key.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = KVEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(v) = self.data[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
                self.data[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + value.len() + 16 + 1;
                self.data.insert(idx, entry)
            }
        }
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = KVEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };
        match self.get_index(key) {
            Ok(idx) => {
                if let Some(value) = self.data[idx].value.as_ref() {
                    self.size -= value.len();
                }
                self.data[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + 16 + 1;
                self.data.insert(idx, entry);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&KVEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.data[idx]);
        }
        None
    }
}
