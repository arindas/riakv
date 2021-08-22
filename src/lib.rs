//!# `riakv`
//![![ci-tests](https://github.com/arindas/riakv/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/arindas/riakv/actions/workflows/ci-tests.yml)
//![![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//! 
//!Log structured, append only, key value store implementation from [Rust In Action](https://www.manning.com/books/rust-in-action) with some enhancements.
//!
//!## Features
//!
//!- Persitent key value store with a hash table index
//!- `crc32` checksum validation for every key value pair stored.
//!- Optionally, persitent index for fast loading
//!- Exhaustive, comprehensive tests

use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};

use std::result;

use std::fs::{File, OpenOptions};
use std::path::Path;

use std::collections::HashMap;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_derive::{Deserialize, Serialize};

/// Type to represent binary content
pub type ByteString = Vec<u8>;

/// Type to represent binary content internally
pub type ByteStr = [u8];

/// Representation of a key value pair
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

/// Generic representation of a key value store in `libriakv`
/// The underlying storage can be any type with the `Read + Write + Seek` trait bounds.
/// The `index` attribute is used to maintain a mapping from the key value pairs to the
/// position at which they are stored in the backing storage file.
#[derive(Debug)]
pub struct RiaKV<F>
where
    F: Read + Write + Seek,
{
    f: F,
    pub index: HashMap<ByteString, u64>,
}

/// Represent the kind of index operation to use for a given `(KeyValuePair, u64)`
/// received during iterating over the contents of the storage file.
pub enum IndexOp {
    Insert(KeyValuePair, u64),
    Delete(KeyValuePair, u64),
    End,
    Nop,
}

impl RiaKV<File> {
    /// Creates a new `RiaKV` instance from a file stored at the given path as th
    /// backing store.
    ///
    /// # Example
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let storage_path = std::path::Path::new("/path/to/some/file.db");
    /// 
    /// match RiaKV::open_from_file_at_path(storage_path) {
    ///     Ok(opened_store) => {}, // use the opened store
    ///     _ => {} // handle failure
    /// };
    /// ```
    pub fn open_from_file_at_path(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        Ok(RiaKV {
            f: f,
            index: HashMap::new(),
        })
    }
}

impl RiaKV<io::Cursor<Vec<u8>>> {
    /// Creates a new `RiakV` instance from an in memory buffer [`Vec<u8>`] with the given
    /// capacity.
    ///
    /// # Example
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let mut store = RiaKV::open_from_in_memory_buffer(5000);
    /// ```
    pub fn open_from_in_memory_buffer(capacity: usize) -> Self {
        RiaKV {
            f: io::Cursor::new(vec![0; capacity]),
            index: HashMap::new(),
        }
    }
}

impl<F> RiaKV<F>
where
    F: Read + Write + Seek,
{
    /// Processes a record from the current position in the underlying storage file.
    /// Every record (key value pair) is stored with the following layout:
    /// ```text
    /// ┌────────────────┬────────────┬──────────────┬────────────────┐
    /// │ crc32 checksum │ key length │ value length │ KeyValuePair{} │
    /// └────────────────┴────────────┴──────────────┴────────────────┘
    /// ```
    ///
    /// Reading a record from the underlying storage occurs in the following steps:
    /// - Read the checksum, key length and value length as 32 bit integers with little endian
    /// format
    /// - Read the next (key length + value length) bytes into a bytestring
    /// - Verify that the crc32 checksum of the data Bytestring read matches with the crc32
    /// checksum read
    /// - Split of the bytestring at key length from the start to obtain the key and the value
    /// - Return `KeyValuePair { key, value }`
    ///
    /// # Example
    /// ```
    /// use std::io;
    /// use libriakv::RiaKV;
    ///
    /// let mut cursor = io::Cursor::new(vec![0; 5000]);
    ///
    /// // .. enter some data into the cursor
    /// 
    /// let maybe_kv = RiaKV::<io::Cursor<Vec<u8>>>::process_record(&mut cursor);
    /// ```
    pub fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;

        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }

        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = crc::crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered: ({:08x}) != {:08x}",
                checksum, saved_checksum
            );
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }

    /// Seeks to the end of the underlying storage file. Any subsequent read should end in EOF.
    pub fn seek_to_end(&mut self) -> io::Result<u64> {
        self.f.seek(SeekFrom::End(0))
    }

    /// For each function for processing all `KeyValuePair{}` instances stored in the underlying
    /// storage.
    ///
    /// The key value entries are processed in the following way:
    /// - First we backup the current position of the underlying storage since it would otherwise
    /// be lost during scanning the entire storafge file
    /// - Next we seek to the start of the storage file
    /// - Now in an infinite loop, during every iteration
    ///     - We seek to the current position
    ///     - We read a record using `RiaKV::process_record`
    ///     - If the record read is not an error, we operate on it using the callback
    ///     - In the case of an error
    ///         - For simple EOF we break out of the loop
    ///         - In the case of any other error, we return Err(err)
    /// - Now if the callback is executed, the return value is used as follows:
    ///     - For IndexOp::Insert the key value pair is inserted into the index
    ///     - For IndexOp::Delete the key value pair is deleted from the index if it existed in the
    ///     index before
    ///     - For Index::Nop we do nothing a continue to the next iteration
    ///     - For Index::End we break out of the loop
    /// - When we exit from the loop, we seek back to the position we saved before entering into
    /// the loop
    /// - We return Ok(())
    ///
    /// # Example
    ///
    /// ```
    /// use libriakv::{RiaKV, IndexOp, ByteString, ByteStr};
    /// use std::io;
    /// use std::io::prelude::*;
    ///
    /// // As used in the impl{} of RiaKV itself
    /// 
    /// fn load<F>(store: &mut RiaKV<F>) -> io::Result<()> where F: Read + Write + Seek {
    ///     store.for_each_kv_entry_in_storage(|kv, position| {
    ///         if kv.value.len() > 0 {
    ///             IndexOp::Insert(kv, position)
    ///         } else {
    ///             IndexOp::Delete(kv, position)
    ///         }
    ///     })
    /// }
    ///
    /// // ...
    ///
    /// fn find<F>(store: &mut RiaKV<F>, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>>
    ///     where F: Read + Write + Seek, {
    ///     
    ///     let mut found: Option<(u64, ByteString)> = None;
    ///
    ///     store.for_each_kv_entry_in_storage(|kv, position| {
    ///         if kv.key == target.to_vec() {
    ///             found = Some((position, kv.value));
    ///             IndexOp::End
    ///         } else {
    ///             IndexOp::Nop
    ///         }
    ///     })?;
    ///
    ///    Ok(found)
    /// } 
    /// ```
    pub fn for_each_kv_entry_in_storage<Func>(&mut self, mut callback: Func) -> io::Result<()>
    where
        Func: FnMut(KeyValuePair, u64) -> IndexOp,
    {
        let mut f = BufReader::new(&mut self.f);
        let previous_position = f.seek(SeekFrom::Current(0))?;
        f.seek(SeekFrom::Start(0))?;

        loop {
            let position = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = RiaKV::<F>::process_record(&mut f);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }

                    _ => return Err(err),
                },
            };

            match callback(kv, position) {
                IndexOp::Insert(kv, position) => {
                    self.index.insert(kv.key, position);
                }
                IndexOp::Delete(kv, _) => {
                    self.index.remove(&kv.key);
                }
                IndexOp::Nop => {}
                IndexOp::End => {
                    break;
                }
            }
        }
        f.seek(SeekFrom::Start(previous_position))?;

        Ok(())
    }

    /// Loads all the key value entries from the underlying storage
    pub fn load(&mut self) -> io::Result<()> {
        self.for_each_kv_entry_in_storage(|kv, position| {
            if kv.value.len() > 0 {
                IndexOp::Insert(kv, position)
            } else {
                IndexOp::Delete(kv, position)
            }
        })
    }

    /// Gets the `KeyValuePair{}` instance stored at the given position in the
    /// underlying storage.
    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let kv = RiaKV::<F>::process_record(&mut f)?;

        Ok(kv)
    }

    /// Get the value for the given key.
    ///
    /// # Example
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let mut store = RiaKV::open_from_in_memory_buffer(5000);
    ///
    /// store.insert(b"key", b"value").expect("insert");
    /// store.get(b"key").expect("get").unwrap();
    /// ```
    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;

        if kv.value.len() > 0 {
            Ok(Some(kv.value))
        } else {
            Ok(None)
        }
    }

    /// Finds the first `KeyValueEntry{}` corresponding to the given `ByteStr` key.
    ///
    /// Note: Since this implementation is an append only, log structured store,
    /// deleted entries will always also have corresponding entries.
    ///
    /// # Example
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let mut store = RiaKV::open_from_in_memory_buffer(5000);
    ///
    /// store.insert(b"key", b"value").expect("insert");
    /// store.find(b"key").expect("find").unwrap();
    /// ```
    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut found: Option<(u64, ByteString)> = None;

        self.for_each_kv_entry_in_storage(|kv, position| {
            if kv.key == target.to_vec() {
                found = Some((position, kv.value));
                IndexOp::End
            } else {
                IndexOp::Nop
            }
        })?;

        Ok(found)
    }

    /// Inserts the given key value pair into the underlying storage and returns the position
    /// in the underlying storage file, it was written at. The index is not updated.
    ///
    /// As mentioned before, the following layout is used for storing the key value pair:
    /// ```text
    /// ┌────────────────┬────────────┬──────────────┬────────────────┐
    /// │ crc32 checksum │ key length │ value length │ KeyValuePair{} │
    /// └────────────────┴────────────┴──────────────┴────────────────┘
    /// ```
    ///
    /// This method is intended to be used in the actual `RiaKV::insert()` implementation.
    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + val_len);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }

        let checksum = crc::crc32::checksum_ieee(&tmp);
        let current_position = f.seek(SeekFrom::Current(0))?;

        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&tmp)?;

        Ok(current_position)
    }

    /// Inserts the given key value pair into the underlying storage and updates the index.
    ///
    /// # Example
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let mut store = RiaKV::open_from_in_memory_buffer(5000);
    /// store.insert(b"key", b"value").expect("insert");
    /// ```
    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    /// Updates the value for the given key by inserting a duplicate entry into the storage and
    /// updating the index.
    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    /// Deletes the value for the given key by inserting a _tombstone_ entry:
    /// 
    /// # Equivalent implementation
    /// ```
    /// use libriakv::RiaKV;
    ///
    /// let mut store = RiaKV::open_from_in_memory_buffer(5000);
    /// store.insert(b"key", b"").expect("delete");
    /// ```
    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}

impl<F> RiaKV<F>
where
    F: Read + Write + Seek,
{
    
    /// Loads the index from the given object implementing the `Read` trait.
    /// This done by deserializing the contents of the file using
    /// `bincode::deserialize_from(reader)` into a `HashMap<ByteString, u64>`.
    ///
    /// The `Read` object is wrapped into a `io::BufReader` instance before
    /// reading the contents.
    pub fn load_index<R: Read>(
        &mut self,
        index_file: &mut R,
    ) -> result::Result<(), bincode::Error> {
        let reader = BufReader::new(index_file);

        match bincode::deserialize_from(reader) {
            Ok(index) => {
                self.index = index;
                Ok(())
            }
            Err(value) => Err(value),
        }
    }

    /// Writes the index into the given object implementing the `Write` trait,
    /// using `bincode::serialize_into(writer, &self.index)`
    ///
    /// The `Write` object is wrapped into a `io::BufWriter` instance before
    /// writing the contents.
    pub fn persist_index<W: Write>(
        &self,
        index_file: &mut W,
    ) -> result::Result<(), bincode::Error> {
        let writer = BufWriter::new(index_file);

        bincode::serialize_into(writer, &self.index)
    }
}

#[cfg(test)]
mod tests {
    use crate::RiaKV;

    #[test]
    fn insert() {
        let mut store = RiaKV::open_from_in_memory_buffer(5000);
        store.insert(b"key", b"value").expect("insert");
    }

    #[test]
    fn insert_get() {
        let mut store = RiaKV::open_from_in_memory_buffer(5000);

        let kv_pairs = [
            (b"12345", b"1qwerrtyui"),
            (b"asdef", b"1zxcvnnnqq"),
            (b"1asdf", b"qwertynnii"),
            (b"1zxcv", b"1lllllpppq"),
            (b"qwert", b"1zxcqqqqee"),
            (b"abjkl", b"1aassddwww"),
            (b"nmkli", b"1qaazzssqq"),
            (b"asdff", b"1ppppkkkkq"),
        ];

        for kv in kv_pairs {
            store.insert(kv.0, kv.1).expect("insert");
        }

        for kv in kv_pairs {
            let value = store.get(kv.0).expect("get").unwrap();
            assert_eq!(value, kv.1.to_vec());
        }
    }

    #[test]
    fn update_get() {
        let mut store = RiaKV::open_from_in_memory_buffer(5000);

        let kv_pairs = [
            (b"12345", b"1qwerrtyui_1"),
            (b"asdef", b"1zxcvnnnqq_1"),
            (b"1asdf", b"qwertynnii_1"),
            (b"1zxcv", b"1lllllpppq_1"),
            (b"qwert", b"1zxcqqqqee_1"),
            (b"abjkl", b"1aassddwww_1"),
            (b"nmkli", b"1qaazzssqq_1"),
            (b"asdff", b"1ppppkkkkq_1"),
            (b"asdef", b"1zxcvnnnqq_2"),
            (b"1asdf", b"qwertynnii_2"),
            (b"1zxcv", b"1lllllpppq_2"),
            (b"abjkl", b"1aassddwww_2"),
            (b"asdef", b"1zxcvnnnqq_3"),
            (b"1zxcv", b"1lllllpppq_3"),
            (b"asdff", b"1ppppkkkkq_3"),
            (b"nmkli", b"1qaazzssqq_4"),
            (b"asdef", b"1zxcvnnnqq_4"),
            (b"asdef", b"1zxcvnnnqq_4"),
        ];

        let final_kv_pairs = [
            (b"12345", b"1qwerrtyui_1"),
            (b"asdef", b"1zxcvnnnqq_4"),
            (b"1asdf", b"qwertynnii_2"),
            (b"1zxcv", b"1lllllpppq_3"),
            (b"qwert", b"1zxcqqqqee_1"),
            (b"abjkl", b"1aassddwww_2"),
            (b"nmkli", b"1qaazzssqq_4"),
            (b"asdff", b"1ppppkkkkq_3"),
        ];

        for kv in kv_pairs {
            store.update(kv.0, kv.1).expect("update");
        }

        for kv in final_kv_pairs {
            let value = store.get(kv.0).expect("get").unwrap();
            assert_eq!(value, kv.1.to_vec());
        }
    }

    #[test]
    fn delete_and_find() {
        let mut store = RiaKV::open_from_in_memory_buffer(5000);

        let kv_pairs = [
            (b"12345", b"1qwerrtyui"),
            (b"asdef", b"1zxcvnnnqq"),
            (b"1asdf", b"qwertynnii"),
            (b"1zxcv", b"1lllllpppq"),
            (b"qwert", b"1zxcqqqqee"),
            (b"abjkl", b"1aassddwww"),
            (b"nmkli", b"1qaazzssqq"),
            (b"asdff", b"1ppppkkkkq"),
        ];

        for kv in kv_pairs {
            store.insert(kv.0, kv.1).expect("insert");
        }

        let kv_pairs_to_delete = [
            (b"abjkl", b"1aassddwww"),
            (b"nmkli", b"1qaazzssqq"),
            (b"asdff", b"1ppppkkkkq"),
        ];

        let kv_pairs_untouched = [
            (b"12345", b"1qwerrtyui"),
            (b"asdef", b"1zxcvnnnqq"),
            (b"1asdf", b"qwertynnii"),
            (b"1zxcv", b"1lllllpppq"),
            (b"qwert", b"1zxcqqqqee"),
        ];

        for kv in kv_pairs_to_delete {
            store.delete(kv.0).expect("delete");
        }

        for kv in kv_pairs_untouched {
            let value = store.get(kv.0).expect("get").unwrap();
            assert_eq!(value, kv.1.to_vec());
        }

        for kv in kv_pairs_to_delete {
            let value = store.get(kv.0).expect("get");
            assert_eq!(value, None);
        }

        for kv in kv_pairs {
            store.find(kv.0).expect("find").unwrap();
        }
    }
}
