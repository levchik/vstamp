use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

pub struct SizeCmd;

impl SizeCmd {
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_static(b"SIZE")
    }
}

pub struct SetCmd {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl SetCmd {
    pub fn into_bytes(self) -> Bytes {
        Bytes::from(format!(
            "S {} {}",
            std::str::from_utf8(&*self.key).unwrap(),
            std::str::from_utf8(&*self.value).unwrap()
        ))
    }
}

pub struct GetCmd {
    pub(crate) key: Bytes,
}

impl GetCmd {
    pub fn into_bytes(self) -> Bytes {
        Bytes::from(format!(
            "G {}",
            std::str::from_utf8(&*self.key).unwrap()
        ))
    }
}

pub struct DeleteCmd {
    pub(crate) key: Bytes,
}

impl DeleteCmd {
    pub fn into_bytes(self) -> Bytes {
        Bytes::from(format!(
            "D {}",
            std::str::from_utf8(&*self.key).unwrap()
        ))
    }
}

/// A record in key-value database, any key can have or not have a value.
#[derive(Debug)]
pub struct KVEntry {
    pub key: Bytes,
    pub value: Option<Bytes>,
}

/// A key-value database, guarded by a Mutex with Arc.
/// Defined here just for convenience.
pub(crate) type GuardedKVApp = Arc<Mutex<KVApp>>;

/// A key-value database, which data is stored in simple Vec
#[derive(Debug)]
pub struct KVApp {
    data: Vec<KVEntry>,
    /// This is size of all data in database, calculated after each operation
    size: usize,
}

/// Const used to define absence of key in database.
const NIL_VALUE: &'static [u8] = b"nil";

impl KVApp {
    /// Returns a new key-value database
    ///
    /// # Examples
    ///
    /// ```
    /// use vstamp::KVApp;
    /// let app = KVApp::new();
    /// ```
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            size: 0,
        }
    }

    /// Applies a command to the database as Bytes, runs command and returns the result.
    ///
    /// # Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use vstamp::KVApp;
    /// let mut app = KVApp::new();
    /// let set_op = Bytes::from_static("S KEY VALUE".as_ref());
    /// let set_result = app.apply(set_op);
    /// assert_eq!(set_result, Bytes::from_static("VALUE".as_ref()));
    /// ```
    pub fn apply(&mut self, request: Bytes) -> Bytes {
        let request_str = std::str::from_utf8(&*request).unwrap().to_string();
        let parts: Vec<String> =
            request_str.split_whitespace().map(String::from).collect();

        let cmd_symbol = parts[0].as_str();
        match cmd_symbol {
            "S" => {
                let cmd = self.parse_set(parts);
                self.set(cmd.key, cmd.value.clone());
                cmd.value
            }
            "G" => {
                let cmd = self.parse_get(parts);
                match self.get(cmd.key) {
                    Some(entry) => match entry.value.as_ref() {
                        Some(value) => value.clone(),
                        None => Bytes::new(),
                    },
                    None => Bytes::from(NIL_VALUE),
                }
            }
            "D" => {
                let cmd = self.parse_delete(parts);
                self.delete(cmd.key);
                Bytes::new()
            }
            "SIZE" => {
                Bytes::from(self.size.to_string())
            }
            _ => unimplemented!(),
        }
    }

    /// Parses a SET command from a vector of strings into a SetCmd struct
    ///
    /// Format: S KEY <VALUE>
    fn parse_set(&mut self, tokens: Vec<String>) -> SetCmd {
        let key = tokens[1].clone();
        match tokens.get(2) {
            None => SetCmd {
                key: Bytes::from(key),
                value: Bytes::new(),
            },
            Some(value) => SetCmd {
                key: Bytes::from(key),
                value: Bytes::from(value.to_owned()),
            },
        }
    }

    /// Parses a GET command from a vector of strings into a GetCmd struct
    ///
    /// Format: G KEY
    fn parse_get(&mut self, tokens: Vec<String>) -> GetCmd {
        let key = tokens[1].clone();
        GetCmd {
            key: Bytes::from(key),
        }
    }

    /// Parses a DELETE command from a vector of strings into a DeleteCmd struct
    ///
    /// Format: D KEY
    fn parse_delete(&mut self, tokens: Vec<String>) -> DeleteCmd {
        let key = tokens[1].clone();
        DeleteCmd {
            key: Bytes::from(key),
        }
    }

    /// Returns how many records are in the database
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns actual data from the database as a whole
    pub fn data(&self) -> &[KVEntry] {
        &self.data
    }

    /// Returns the size of the database
    pub fn size(&self) -> usize {
        self.size
    }

    /// Gets index of a key in database
    fn get_index(&self, key: Bytes) -> Result<usize, usize> {
        self.data.binary_search_by_key(&key, |e| e.key.clone())
    }

    /// Sets a value to a key in database, updates value if key already exists
    ///
    /// Updates size of the database
    pub fn set(&mut self, key: Bytes, value: Bytes) {
        let entry = KVEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
        };

        match self.get_index(key.clone()) {
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
                self.size += key.len() + value.len();
                self.data.insert(idx, entry)
            }
        }
    }

    /// Removes a key from database, updates size of the database
    pub fn delete(&mut self, key: Bytes) {
        if let Ok(idx) = self.get_index(key.clone()) {
            if let Some(value) = self.data[idx].value.as_ref() {
                self.size -= value.len();
            }
            self.size -= key.len();
            self.data.remove(idx);
        }
    }

    /// Gets a record from a key in database, returns None if key doesn't exist
    pub fn get(&self, key: Bytes) -> Option<&KVEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.data[idx]);
        }
        None
    }
}
