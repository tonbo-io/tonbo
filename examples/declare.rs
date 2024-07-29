use morseldb::morsel_record;

// Tips: must be public
#[morsel_record]
pub struct Post {
    #[primary_key]
    name: String,
}

fn main() {
    println!("Hello, world!");
}
