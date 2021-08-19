
#[cfg(target_os="windows")]
const USAGE: &str = "
Usage:
    riakv_mem.exe FILE get KEY
    riakv_mem.exe FILE delete KEY
    riakv_mem.exe FILE insert KEY VALUE
    riakv_mem.exe FILE update KEY VALUE
";

#[cfg(target_os="linux")]
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

    let _path = std::path::Path::new(fname); 


    match action {
        "get" => {},
        "delete" => {},
        "insert" => {},
        "update" => {},
        _ => {}
    }
}