use libriakv::RiaKV;

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

#[cfg(target_os = "windows")]
const USAGE: &str = "
CLI client for RiaKV key value store with persistent index.

Usage:
    riakv_mem.exe STORAGE_FILE INDEX_FILE get KEY
    riakv_mem.exe STORAGE_FILE INDEX_FILE delete KEY
    riakv_mem.exe STORAGE_FILE INDEX_FILE insert KEY VALUE
    riakv_mem.exe STORAGE_FILE INDEX_FILE update KEY VALUE
";

#[cfg(target_os = "linux")]
const USAGE: &str = "
CLI client for RiaKV key value store with persistent index.

Usage:
    riakv_mem STORAGE_FILE INDEX_FILE get KEY
    riakv_mem STORAGE_FILE INDEX_FILE delete KEY
    riakv_mem STORAGE_FILE INDEX_FILE insert KEY VALUE
    riakv_mem STORAGE_FILE INDEX_FILE update KEY VALUE
";

fn index_file_from_path(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let storage_fname = args.get(1).expect(&USAGE);
    let index_fname = args.get(2).expect(&USAGE);

    let action = args.get(3).expect(&USAGE).as_ref();
    let key = args.get(4).expect(&USAGE).as_ref();
    let maybe_value = args.get(5);

    let storage_path = Path::new(storage_fname);
    let mut store = RiaKV::open_from_file_at_path(storage_path).expect("unable to open file");

    let index_path = Path::new(index_fname);
    let mut index_file = index_file_from_path(index_path).expect("unable to open index file");
    store
        .load_index(&mut index_file)
        .expect("unable to deserialize index");

    match action {
        "get" => match store.get(key).unwrap() {
            None => eprintln!("{:?} not found", key),
            Some(value) => println!("{:?}", value),
        },

        "delete" => store.delete(key).unwrap(),

        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap()
        }

        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap()
        }
        _ => eprintln!("{}", &USAGE),
    }

    store
        .persist_index(&mut index_file)
        .expect("unable to serialize index");
}
