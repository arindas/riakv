use libriakv::RiaKV;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    riakv_mem.exe FILE get KEY
    riakv_mem.exe FILE delete KEY
    riakv_mem.exe FILE insert KEY VALUE
    riakv_mem.exe FILE update KEY VALUE
";

#[cfg(target_os = "linux")]
const USAGE: &str = "
Usage:
    riakv_mem FILE get KEY
    riakv_mem FILE delete KEY
    riakv_mem FILE insert KEY VALUE
    riakv_mem FILE update KEY VALUE
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);

    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = std::path::Path::new(fname);
    let mut store = RiaKV::open_from_file_at_path(path).expect("unable to open file");
    store.load().expect("unable to load data");

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
}
