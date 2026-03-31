#![cfg(all(feature = "wasm-runtime", feature = "wasm-build"))]

use arrow_udf_runtime::wasm::{build::*, Runtime};

#[test]
fn test_build() {
    let manifest = r#"
[dependencies]
chrono = "0.4"
"#;

    let script = r#"
use arrow_udf::function;

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
"#;
    let binary = build(manifest, script).unwrap();

    let runtime = Runtime::new(&binary).unwrap();
    assert!(runtime.functions().any(|f| f == "gcd(int4,int4)->int4"));

    // build again with offline mode
    test_build_offline();
}

#[test]
fn test_build_error() {
    let err = build("??", "").unwrap_err();
    assert!(err.to_string().contains("invalid key"));
}

fn test_build_offline() {
    let mut opt = BuildOpts::default();
    opt.offline = true;
    opt.script = r#"
        use arrow_udf::function;

        #[function("gcd(int, int) -> int")]
        fn gcd(mut a: i32, mut b: i32) -> i32 {
            while b != 0 {
                (a, b) = (b, a % b);
            }
            a
        }
    "#
    .into();
    let binary = build_with(&opt).unwrap();

    let runtime = Runtime::new(&binary).unwrap();
    assert!(runtime.functions().any(|f| f == "gcd(int4,int4)->int4"));
}
