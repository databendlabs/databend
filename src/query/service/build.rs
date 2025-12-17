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
use std::path::PathBuf;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_dir = Path::new(&manifest_dir).join("src");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    let mut entries = collect_impls(&src_dir);
    entries.sort_by(|a, b| a.1.cmp(&b.1));

    let impls_out = out_dir.join("physical_plan_impls.rs");
    write_impls(&impls_out, &entries);

    let dispatch_out = out_dir.join("physical_plan_dispatch.rs");
    write_dispatch(&dispatch_out, &entries);

    println!("cargo:rerun-if-changed={}", src_dir.display());
}

fn collect_impls(src_dir: &Path) -> Vec<(Option<String>, String, String)> {
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
            if let Some(e) = parse_impl_line(src_dir, entry.path(), line) {
                entries.push(e);
            }
        }
    }

    entries
}

fn parse_impl_line(
    src_root: &Path,
    path: &Path,
    line: &str,
) -> Option<(Option<String>, String, String)> {
    // match `impl ... IPhysicalPlan for Type ... {`
    let marker = "IPhysicalPlan for";
    let idx = line.find(marker)?;
    let after = &line[idx + marker.len()..];
    let type_part = after
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches('{')
        .trim_end_matches(';');
    if type_part.is_empty() {
        return None;
    }

    let type_name = type_part
        .split("::")
        .last()
        .unwrap_or(type_part)
        .split('<')
        .next()
        .unwrap_or(type_part)
        .to_string();

    let mut module_path = module_path_from_file(src_root, path);
    let mut meta = None;

    if type_name == "StackDepthPlan" && path.ends_with("physical_plan.rs") {
        module_path.push_str("::tests");
        meta = Some("#[cfg(test)]".to_string());
    }

    let full_path = format!("{module_path}::{type_part}");

    Some((meta, type_name, full_path))
}

fn module_path_from_file(src_root: &Path, path: &Path) -> String {
    let mut module_path = String::from("crate");
    let rel = path
        .strip_prefix(src_root)
        .unwrap_or(path)
        .with_extension("");
    for comp in rel.components() {
        let c = comp.as_os_str().to_string_lossy();
        module_path.push_str("::");
        module_path.push_str(&c);
    }
    module_path
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
