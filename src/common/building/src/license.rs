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

use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Result;
use cargo_metadata::DepKindInfo;
use cargo_metadata::DependencyKind;
use cargo_metadata::Metadata;
use cargo_metadata::MetadataCommand;
use cargo_metadata::Node;
use cargo_metadata::NodeDep;
use cargo_metadata::Package;
use cargo_metadata::PackageId;
use cargo_metadata::semver::Version;

fn normalize(license_string: &str) -> String {
    let mut list: Vec<&str> = license_string
        .split('/')
        .flat_map(|e| e.split(" OR "))
        .map(str::trim)
        .collect();
    list.sort_unstable();
    list.dedup();
    list.join(" OR ")
}

fn get_node_name_filter(metadata: &Metadata, opt: &GetDependenciesOpt) -> Result<HashSet<String>> {
    let mut filter = HashSet::new();

    let roots = if let Some(root) = metadata.root_package() {
        vec![root]
    } else {
        metadata.workspace_packages()
    };

    if opt.root_only {
        for root in roots {
            filter.insert(root.name.clone());
        }
        return Ok(filter);
    }

    if opt.direct_deps_only {
        for root in roots {
            filter.insert(root.name.clone());
            for package in root.dependencies.iter() {
                filter.insert(package.name.clone());
            }
        }
    }
    Ok(filter)
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct DependencyDetails {
    pub name: String,
    pub version: Version,
    pub authors: Option<String>,
    pub repository: Option<String>,
    pub license: Option<String>,
    pub license_file: Option<String>,
    pub description: Option<String>,
}

impl DependencyDetails {
    #[must_use]
    pub fn new(package: &Package) -> Self {
        let authors = if package.authors.is_empty() {
            None
        } else {
            Some(package.authors.to_owned().join("|"))
        };
        Self {
            name: package.name.to_owned(),
            version: package.version.to_owned(),
            authors,
            repository: package.repository.to_owned(),
            license: package.license.as_ref().map(|s| normalize(s)),
            license_file: package.license_file.to_owned().map(|f| f.into_string()),
            description: package
                .description
                .to_owned()
                .map(|s| s.trim().replace('\n', " ")),
        }
    }
}

#[derive(Default)]
pub struct GetDependenciesOpt {
    pub avoid_dev_deps: bool,
    pub avoid_build_deps: bool,
    pub direct_deps_only: bool,
    pub root_only: bool,
}

pub fn get_dependencies_from_cargo_lock(
    metadata_command: MetadataCommand,
    opt: GetDependenciesOpt,
) -> Result<Vec<DependencyDetails>> {
    let metadata = metadata_command.exec()?;

    let filter = get_node_name_filter(&metadata, &opt)?;

    let connected = {
        let resolve = metadata.resolve.as_ref().expect("missing `resolve`");

        let deps = resolve
            .nodes
            .iter()
            .map(|Node { id, deps, .. }| (id, deps))
            .collect::<HashMap<_, _>>();

        let missing_dep_kinds = deps
            .values()
            .flat_map(|d| d.iter())
            .any(|NodeDep { dep_kinds, .. }| dep_kinds.is_empty());

        if missing_dep_kinds && opt.avoid_dev_deps {
            eprintln!("warning: Cargo 1.41+ is required for `--avoid-dev-deps`");
        }
        if missing_dep_kinds && opt.avoid_build_deps {
            eprintln!("warning: Cargo 1.41+ is required for `--avoid-build-deps`");
        }

        let neighbors = |package_id: &PackageId| {
            deps[package_id]
                .iter()
                .filter(|NodeDep { dep_kinds, .. }| {
                    missing_dep_kinds
                        || dep_kinds.iter().any(|DepKindInfo { kind, .. }| {
                            *kind == DependencyKind::Normal
                                || !opt.avoid_dev_deps && *kind == DependencyKind::Development
                                || !opt.avoid_build_deps && *kind == DependencyKind::Build
                        })
                })
                .map(|NodeDep { pkg, .. }| pkg)
        };

        let mut connected = HashSet::new();
        let stack = &mut if let Some(root) = &resolve.root {
            vec![root]
        } else {
            metadata.workspace_members.iter().collect()
        };
        while let Some(package_id) = stack.pop() {
            if connected.insert(package_id) {
                stack.extend(neighbors(package_id));
            }
        }
        connected
    };

    let mut detailed_dependencies = metadata
        .packages
        .iter()
        .filter(|p| connected.contains(&p.id))
        .filter(|p| filter.is_empty() || filter.contains(&p.name))
        .map(DependencyDetails::new)
        .collect::<Vec<_>>();
    detailed_dependencies.sort_unstable();
    Ok(detailed_dependencies)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detailed() {
        let cmd = MetadataCommand::new();
        let detailed_dependencies =
            get_dependencies_from_cargo_lock(cmd, GetDependenciesOpt::default()).unwrap();
        assert!(!detailed_dependencies.is_empty());
        for detailed_dependency in detailed_dependencies.iter() {
            assert!(
                detailed_dependency.license.is_some() || detailed_dependency.license_file.is_some()
            );
        }
    }
}
