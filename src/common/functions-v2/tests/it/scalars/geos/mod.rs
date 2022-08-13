use std::io::Write;

use goldenfile::Mint;

use crate::scalars::run_ast;

#[test]
fn test_geos() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geo.txt").unwrap();

    test_geo_to_h3(file);
}

fn test_geo_to_h3(file: &mut impl Write) {
    run_ast(file, "geo_to_h3(37.79506683, 55.71290588, 15)", &[]);
}
