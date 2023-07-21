use std::io::Write;

use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_geo_h3() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geo_h3.txt").unwrap();

    test_h3_to_geo(file);
}

fn test_h3_to_geo(file: &mut impl Write) {
    run_ast(file, "geo_to_h3(37.79506683, 55.71290588, 15)", &[]);
}
