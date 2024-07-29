use morseldb::morsel_record;

// Tips: must be public
#[morsel_record]
pub struct Post {
    #[primary_key]
    name: String,
    vu8: u8,
    vu16: u16,
    vu32: u32,
    vu64: u64,
    vi8: i8,
    vi16: i16,
    vi32: i32,
    vi64: i64,
    vbool: bool,
}

fn main() {
    println!("Hello, world!");
}
