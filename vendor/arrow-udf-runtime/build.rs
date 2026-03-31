fn main() {
    if std::env::var("CARGO_FEATURE_PYTHON_RUNTIME").is_ok() {
        // skip the check on docs.rs. it only has python 3.10.
        if std::env::var("DOCS_RS").is_ok() {
            return;
        }
        let version = pyo3_build_config::get().version;
        let minimum_version = pyo3_build_config::PythonVersion {
            major: 3,
            minor: 12,
        };
        assert!(
            version >= minimum_version,
            "arrow-udf-runtime requires Python 3.12 or later, but found {}\nhint: you can set `PYO3_PYTHON` environment varibale, e.g. `PYO3_PYTHON=python3.12`",
            version
        );
    }
}
