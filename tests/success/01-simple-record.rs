use tonbo::typed as t;

#[t::record]
#[derive(Debug, Default)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

fn main() {}
