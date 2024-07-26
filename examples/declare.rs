use morseldb::morsel_record;

#[morsel_record]
struct Post {
    #[primary_key]
    name: String,
}

fn main() {
    println!("Hello, world!");
}
