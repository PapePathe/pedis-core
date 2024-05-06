//! This crate defines the behaviour of pedis key value store.
#![warn(missing_docs)]
use std::{rc::Rc, sync::Arc, sync::RwLock};

/// Defines the behaviour of the redis command handlers
///
/// Creating a command handler is as simple as creating
/// a stateless struct and generating the boilerplate to
/// implement the interface.
///
/// # Errors
///
/// # Examples
///
/// ```
/// use pedis_core::RedisCommandHandler;
/// use pedis_core::IStore;
/// use pedis_core::RedisCommand;
/// ```
///
/// # Todo!
///
/// - [x] Add basic handler that has access to a store with only get and set methods.
/// - [ ] Add a handler that provides a store with the ability to use `del` method.
/// - [ ] Return a `Vec<u8>` instead of `String`
/// - [ ] Return a `Result<Vec<u8>, CommandEror>` so that call may decide pot processing
///       the error and sending a resp error to the client.
/// - [ ] Define which endpoints are authenticated
/// - [ ] Allow each handler to document itself
/// - [ ]
///
pub trait RedisCommandHandler {
    /// Executes a single redis command using a read write
    /// locked store if necessary.
    ///
    fn exec(&self, _: AsyncLockedStore, _: Rc<RedisCommand>) -> String;
}

pub type AsyncLockedStore<'a> = Arc<RwLock<&'a mut (dyn IStore + Send + Sync)>>;

/// Encapsulate a redis command
#[derive(Debug, Copy, Clone)]
pub struct RedisCommand<'a> {
    cmd: &'a str,
}

impl<'a> RedisCommand<'a> {
    /// Initialize a new command from a resp string
    pub fn new(cmd: &'a str) -> Self {
        Self { cmd }
    }
    /// Parses the command and returns a vector of strings
    pub fn params(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![];
        let binding = self.cmd;
        let elems: Vec<&str> = binding.split("\r\n").collect::<Vec<_>>();
        for (idx, pat) in elems[1..].iter().enumerate() {
            if idx.rem_euclid(2) == 0 {
                continue;
            }
            args.push(pat.to_string())
        }
        args.clone()
    }
    /// Extracts the name of the command and returns it.
    pub fn name(&self) -> String {
        self.params()[0].clone().to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args() {
        let c = RedisCommand::new("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nHello World\r\n");
        assert_eq!(vec!["SET", "key", "Hello World"], c.params());
    }

    #[test]
    fn test_name() {
        let c = RedisCommand::new("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nHello World\r\n");
        assert_eq!("set", c.name());
    }
}

use std::collections::HashMap;
use std::fmt::{Debug, Display};

/// Store errors
#[derive(Debug, PartialEq)]
pub enum StoreError {
    /// Error returned when a key was not found on the store
    KeyNotFoundError,
    /// Error raised when trying to access a key but the kind of data does not match
    KeyMismatchError(String),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFoundError => {
                write!(f, "-ERR key not found")
            }
            Self::KeyMismatchError(m) => {
                write!(f, "-ERR {:}", m)
            }
        }
    }
}

type StoreResult<T> = Result<T, StoreError>;

/// Defines the storage interface
pub trait IStore {
    /// Set given value using specified key
    fn set(&mut self, k: String, v: Value) -> StoreResult<()>;
    /// Retrieves a key from the store
    fn get(&self, k: String, vk: ValueKind) -> StoreResult<&Value>;
}

/// Default implementation of the store trait
#[derive(Default)]
pub struct RedisStore {
    store: HashMap<String, Value>,
}
impl IStore for RedisStore {
    fn set(&mut self, k: String, v: Value) -> StoreResult<()> {
        self.store.insert(k, v);
        Ok(())
    }
    fn get(&self, k: String, vk: ValueKind) -> StoreResult<&Value> {
        match self.store.get(&k.clone()) {
            Some(value) => {
                if value.kind == vk {
                    return Ok(value);
                }

                Err(StoreError::KeyMismatchError(
                    "key xxx does not match yyy".to_string(),
                ))
            }
            None => Err(StoreError::KeyNotFoundError),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{RedisStore, StoreError, Value, ValueKind};
    use IStore;

    #[test]
    fn test_store_get_set() {
        let mut s = RedisStore::default();
        let value = Value::new_string("hello pedis".to_string().as_bytes().to_vec());
        let set_result = s.set("key:001".to_string(), value);
        assert_eq!(set_result, Result::Ok(()));

        let expected_value = Value::new_string("hello pedis".to_string().as_bytes().to_vec());
        let get_result = s.get("key:001".to_string(), ValueKind::String);
        assert_eq!(Result::Ok(&expected_value), get_result);

        let get_key_kind_mistmatch_result = s.get("key:001".to_string(), ValueKind::Map);
        assert_eq!(
            Err(StoreError::KeyMismatchError(
                "key xxx does not match yyy".to_string()
            )),
            get_key_kind_mistmatch_result
        );

        let get_key_not_found_result = s.get("key:013".to_string(), ValueKind::String);
        assert_eq!(Err(StoreError::KeyNotFoundError), get_key_not_found_result);
    }
}

/// Represents a value in our storage interface
#[derive(PartialEq)]
pub struct Value {
    /// The kind of data stored in this value
    pub kind: ValueKind,
    /// The data as an array of bytes
    pub data: Vec<u8>, //    created_at: u64,
                       //    last_read_at: u64,
                       //    updated_at: u64,
                       //    expires_at: u64
}

impl Value {
    /// Creates a new value with the desired ValueKind
    pub fn new(data: Vec<u8>, kind: ValueKind) -> Self {
        Self { kind, data }
    }
    /// Create a new value of kind string
    pub fn new_string(data: Vec<u8>) -> Self {
        Self {
            kind: ValueKind::String,
            data,
        }
    }
    /// Create a new value of kind map
    pub fn new_map(data: Vec<u8>) -> Self {
        Self {
            kind: ValueKind::Map,
            data,
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "k={:} len={:}", self.kind, self.data.len())
    }
}

/// Represents the kind of values in the pedis store
#[derive(PartialEq)]
pub enum ValueKind {
    /// Used when storing simple strings
    String,
    /// Used when storing data as a map
    Map,
    /// Used when storing json
    Json,
    /// Used when storing lists
    List,
}

impl Display for ValueKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ValueKind::Map => {
                write!(f, "map")
            }
            ValueKind::Json => {
                write!(f, "json")
            }
            ValueKind::List => {
                write!(f, "list")
            }
            ValueKind::String => {
                write!(f, "string")
            }
        }
    }
}

/// Mock store for testing purposes
pub struct Teststore {
    /// Allow the consumer to raise an error while running the tests
    pub err: bool,
}
impl IStore for Teststore {
    fn set(&mut self, _: String, _: Value) -> Result<(), StoreError> {
        if self.err {
            return Err(StoreError::KeyNotFoundError);
        }
        Ok(())
    }
    fn get(&self, _: String, _: ValueKind) -> Result<&Value, StoreError> {
        todo!()
    }
}
