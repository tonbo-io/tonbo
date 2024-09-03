use tonbo_macros::tonbo_record;

#[tonbo_record]
pub struct User {
    #[primary_key]
    name: String,
    email: Option<String>,
    age: u8,
}

fn main() {}
