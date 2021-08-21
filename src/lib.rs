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
    End,
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

    pub fn load(&mut self) -> io::Result<()> {
        self.for_each_kv_entry_in_storage(|kv, position| {
            if kv.value.len() > 0 {
                IndexOp::Insert(kv, position)
            } else {
                IndexOp::Delete(kv, position)
            }
        })
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

        if kv.value.len() > 0 {
            Ok(Some(kv.value))
        } else {
            Ok(None)
        }
    }

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
