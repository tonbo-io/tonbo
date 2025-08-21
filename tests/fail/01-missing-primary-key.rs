use tonbo::typed as t;

#[t::record]
#[derive(Debug)]
pub struct User {
    name: String,
    email: Option<String>,
    age: u8,
}

fn main() {}
