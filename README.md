# `riakv`
[![ci-tests](https://github.com/arindas/riakv/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/arindas/riakv/actions/workflows/ci-tests.yml)

Log structured, append only, key value store implementation from https://www.manning.com/books/rust-in-action with some enhancements.

## Features

- [x] Persitent key value store with a hash table index
- [x] Optionally, persitent index for fast loading
- [x] Exhaustive, comprehensive tests

## Design enhancements

### Generic storage
The underlying storage used by the key value store is completely generic. It is subject to `Read + Write + Seek` trait bounds.
```rust
#[derive(Debug)]
pub struct RiaKV<F>
where
    F: Read + Write + Seek,
{
    f: F,
    pub index: HashMap<ByteString, u64>,
}
```

This allows for creation of key value store instances from an in-memory buffer:
```rust
impl RiaKV<io::Cursor<Vec<u8>>> {
    pub fn open_from_in_memory_buffer(capacity: usize) -> Self {
        RiaKV {
            f: io::Cursor::new(vec![0; capacity]),
            index: HashMap::new(),
        }
    }
}
```

### Refactors in iteration over key value pairs stored in file
Instead of duplicating iteration code in `RiaKV::find` and `RiaKV::load`, we refactor the loop
into `RiaKV::for_each`. This method accepts a callback to operate on the key value pair
received in every iteration. Finally, the callback returns an enum which specified how to
update the store hashtable index using the key value pair.

This is implemented as follows. First, we have an index operation type:
```rust
pub enum IndexOp {
    Insert(KeyValuePair, u64),
    Delete(KeyValuePair, u64),
    End,
    Nop,
}
```

Next we define the for each function:
```rust
pub fn for_each_kv_entry_in_storage<Func>(&mut self, mut callback: Func) -> io::Result<()>
    where
        Func: FnMut(KeyValuePair, u64) -> IndexOp,
    { ...
```

As we can see the callback take the key value pair, and its position in the file, and
returns an index operation. Now in the iteration step, we match on the value returned
by the callback and perform the required index operation.

```rust
    loop { 

        ...
        
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
        
        ...
```

This function is used as follows:
```rust
pub fn load(&mut self) -> io::Result<()> {
    self.for_each_kv_entry_in_storage(|kv, position| {
        if kv.value.len() > 0 {
            IndexOp::Insert(kv, position)
        } else {
            IndexOp::Delete(kv, position)
        }
    })
}

// ...

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
```

## Building

After cloning the repository, simply run `cargo build --release` from the project root. This project
provides two binaries `riakv_mem` and `riakv_disk`. The _* suffix specifies whether the index is
persisted in the disk or not.
