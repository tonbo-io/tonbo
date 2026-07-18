mod model {
    use tonbo::prelude::*;

    #[derive(Union)]
    pub(crate) enum Value {
        Int(i64),
        Text(String),
    }

    #[derive(Record)]
    pub(crate) struct Row {
        #[metadata(k = "tonbo.key", v = "true")]
        id: String,
        value: Value,
    }

    pub(crate) fn field_count() -> usize {
        Row::schema().fields().len()
    }
}

fn main() {
    assert_eq!(model::field_count(), 2);
}
