// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;

fn main() {
    // Keep build script rerun behavior explicit for the feature list we expose.
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=OPT_LEVEL");

    // Features declared in `src/query/service/Cargo.toml`.
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_SIMD");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_PYTHON_UDF");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_DISABLE_INITIAL_EXEC_TLS");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_JEMALLOC");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_MEMORY_PROFILING");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_STORAGE_HDFS");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_IO_URING");

    let mut features = env::vars()
        .filter_map(|(k, v)| {
            if !k.starts_with("CARGO_FEATURE_") {
                return None;
            }
            if v != "1" {
                return None;
            }
            let name = k
                .trim_start_matches("CARGO_FEATURE_")
                .to_ascii_lowercase()
                .replace('_', "-");
            Some(name)
        })
        .collect::<Vec<_>>();

    features.sort();
    features.dedup();

    let features = features.join(",");
    println!("cargo:rustc-env=DATABEND_QUERY_CARGO_FEATURES={features}");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_dir = Path::new(&manifest_dir).join("src");
    let out_dir = Path::new(&env::var("OUT_DIR").expect("OUT_DIR not set")).to_path_buf();

    let mut entries = collect_registrations(&src_dir);
    entries.sort_by(|a, b| a.1.cmp(&b.1));

    let out_path = out_dir.join("physical_plan_impls.rs");
    write_impls(&out_path, &entries);

    let dispatch_out = out_dir.join("physical_plan_dispatch.rs");
    write_dispatch(&dispatch_out, &entries);

    println!("cargo:rerun-if-changed={}", src_dir.display());
}

fn collect_registrations(src_dir: &Path) -> Vec<(Option<String>, String, String)> {
    let mut entries = Vec::new();

    for entry in walkdir::WalkDir::new(src_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }

        let content = fs::read_to_string(entry.path())
            .unwrap_or_else(|e| panic!("read {}: {e}", entry.path().display()));

        for line in content.lines() {
            if let Some(pos) = line.find("register_physical_plan!") {
                let rest = &line[pos..];
                if let Some((variant, path)) = parse_registration(rest) {
                    entries.push((None, variant, path));
                }
            }
        }
    }

    // test-only plan variant
    entries.push((
        Some("#[cfg(test)]".to_string()),
        "StackDepthPlan".to_string(),
        "crate::physical_plans::physical_plan::tests::StackDepthPlan".to_string(),
    ));

    entries
}

fn parse_registration(line: &str) -> Option<(String, String)> {
    let start = line.find('(')? + 1;
    let end = line.find(')')?;
    let body = &line[start..end];
    let mut parts = body.split("=>").map(str::trim);
    let variant = parts.next()?.trim_end_matches(',');
    let path = parts.next()?.trim_end_matches(',');

    Some((variant.to_string(), path.to_string()))
}

fn write_impls(out_path: &Path, entries: &[(Option<String>, String, String)]) {
    let mut out = String::new();
    out.push_str("define_physical_plan_impl!(\n");

    for (meta, variant, path) in entries {
        if let Some(meta) = meta {
            out.push_str("    ");
            out.push_str(meta);
            out.push('\n');
        }
        out.push_str("    ");
        out.push_str(variant);
        out.push_str(" => ");
        out.push_str(path);
        out.push_str(",\n");
    }

    out.push_str(");\n");

    let mut file = fs::File::create(out_path).expect("create physical_plan_impls.rs");
    file.write_all(out.as_bytes())
        .expect("write physical_plan_impls.rs");
}

fn write_dispatch(out_path: &Path, entries: &[(Option<String>, String, String)]) {
    let mut out = String::new();

    out.push_str("macro_rules! dispatch_plan_ref {\n");
    out.push_str("    ($s:expr, $plan:ident => $body:expr) => {\n");
    out.push_str("        match $s.inner.as_ref() {\n");
    for (meta, variant, _) in entries {
        if let Some(meta) = meta {
            out.push_str("            ");
            out.push_str(meta);
            out.push('\n');
        }
        out.push_str("            PhysicalPlanImpl::");
        out.push_str(variant);
        out.push_str("($plan) => $body,\n");
    }
    out.push_str("        }\n");
    out.push_str("    };\n");
    out.push_str("}\n\n");

    out.push_str("macro_rules! dispatch_plan_mut {\n");
    out.push_str("    ($s:expr, $plan:ident => $body:expr) => {\n");
    out.push_str("        match $s.inner.as_mut() {\n");
    for (meta, variant, _) in entries {
        if let Some(meta) = meta {
            out.push_str("            ");
            out.push_str(meta);
            out.push('\n');
        }
        out.push_str("            PhysicalPlanImpl::");
        out.push_str(variant);
        out.push_str("($plan) => $body,\n");
    }
    out.push_str("        }\n");
    out.push_str("    };\n");
    out.push_str("}\n");

    let mut file = fs::File::create(out_path).expect("create physical_plan_dispatch.rs");
    file.write_all(out.as_bytes())
        .expect("write physical_plan_dispatch.rs");
}
