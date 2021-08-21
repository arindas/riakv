use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};

use std::fs::{File, OpenOptions};
use std::path::Path;

use std::collections::HashMap;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_derive::{Deserialize, Serialize};

type ByteString = Vec<u8>;

type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct RiaKV<F>
where
    F: Read + Write + Seek,
{
    f: F,
    pub index: HashMap<ByteString, u64>,
}

pub enum IndexOp {
    Insert(KeyValuePair, u64),
    Delete(KeyValuePair, u64),
    Nop,
}

impl RiaKV<File> {
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
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
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

    pub fn seek_to_end(&mut self) -> io::Result<u64> {
        self.f.seek(SeekFrom::End(0))
    }

    pub fn for_each_kv_entry_in_storage<Func>(&mut self, mut callback: Func) -> io::Result<()>
    where
        Func: FnMut(KeyValuePair, u64) -> IndexOp,
    {
        let mut f = BufReader::new(&mut self.f);

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
            }
        }

        Ok(())
    }

    pub fn load(&mut self) -> io::Result<()> {
        self.for_each_kv_entry_in_storage(|kv, position| IndexOp::Insert(kv, position))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let kv = RiaKV::<F>::process_record(&mut f)?;

        Ok(kv)
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;

        Ok(Some(kv.value))
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut found: Option<(u64, ByteString)> = None;

        self.for_each_kv_entry_in_storage(|kv, position| {
            if kv.key == target {
                found = Some((position, kv.value));
            }

            IndexOp::Nop
        })?;

        Ok(found)
    }

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
        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;

        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&tmp)?;

        Ok(current_position)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
