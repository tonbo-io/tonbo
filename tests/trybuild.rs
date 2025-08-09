#[test]
fn build_test() {
    let t = trybuild::TestCases::new();
    t.pass("tests/success/*.rs");
}

#[test]
fn fail_build_test() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fail/*.rs");
}
