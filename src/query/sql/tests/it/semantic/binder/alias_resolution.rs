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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::plans::Plan;
use serde::Deserialize;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_outcome;
use crate::framework::golden::write_case_title;

const ALIAS_MAIN_CONSUMERS: &[&str] = &[
    "S0", "G0", "G1", "G2", "G3", "G4", "G5", "P0", "P1", "P2", "O0", "A0", "W0", "B0",
];

const ALIAS_MAIN_NAME_SHAPES: &[&str] = &["N0", "N1", "N2", "N3", "N4", "N5"];

const ALIAS_MAIN_PRODUCERS: &[&str] = &["I0", "S0", "S1", "S2", "S3", "S4", "G0", "N0"];

const ALIAS_MATRIX_DIR: &str = "tests/it/semantic/binder/alias_resolution_matrices";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixFile {
    matrix: String,
    local_dimensions: Vec<AliasMatrixLocalDimension>,
    regions: Vec<AliasMatrixRegion>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixLocalDimension {
    name: String,
    values: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixRegion {
    description: String,
    main: AliasMatrixMainSelector,
    #[serde(default)]
    excluded: bool,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    local: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    masks: Vec<AliasMatrixMask>,
    #[serde(default)]
    cases: Vec<AliasMatrixYamlCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixMainSelector {
    consumers: Vec<String>,
    name_shapes: Vec<String>,
    producers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixMask {
    when: AliasMatrixMaskSelector,
    reason: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixMaskSelector {
    #[serde(default)]
    main: AliasMatrixMainMask,
    #[serde(default)]
    local: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixMainMask {
    consumers: Option<Vec<String>>,
    name_shapes: Option<Vec<String>>,
    producers: Option<Vec<String>>,
}

#[derive(Debug)]
struct AliasMatrixGeneratedCoordinate {
    key: String,
    consumer: String,
    name_shape: String,
    producer: String,
    local: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixYamlCase {
    key: String,
    name: String,
    setup_sqls: Vec<String>,
    runs: Vec<AliasMatrixYamlRun>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixYamlRun {
    #[serde(default)]
    settings: AliasMatrixYamlSettings,
    sql: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasMatrixYamlSettings {
    #[serde(default)]
    enable_group_by_column_first: bool,
}

impl AliasMatrixFile {
    fn load_all() -> Vec<Self> {
        let matrix_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(ALIAS_MATRIX_DIR);
        let mut paths = fs::read_dir(&matrix_dir)
            .unwrap_or_else(|err| {
                panic!(
                    "failed to read alias matrix yaml directory {}: {err}",
                    matrix_dir.display()
                )
            })
            .map(|entry| {
                entry
                    .unwrap_or_else(|err| panic!("failed to read alias matrix yaml entry: {err}"))
                    .path()
            })
            .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
            .collect::<Vec<_>>();
        paths.sort();

        assert!(
            !paths.is_empty(),
            "alias matrix yaml directory {} has no yaml files",
            matrix_dir.display()
        );

        let mut matrices = paths
            .into_iter()
            .map(|path| {
                let name = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or_else(|| {
                        panic!(
                            "alias matrix yaml path has no UTF-8 file stem: {}",
                            path.display()
                        )
                    });
                let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                    panic!("failed to read alias matrix yaml {}: {err}", path.display())
                });
                let matrix =
                    serde_yaml::from_str::<AliasMatrixFile>(&content).unwrap_or_else(|err| {
                        panic!("failed to parse alias matrix yaml {name}: {err}")
                    });
                assert_eq!(
                    matrix.matrix, name,
                    "alias matrix yaml file name and matrix field differ"
                );
                matrix
            })
            .collect::<Vec<_>>();

        matrices.sort_by(|left, right| {
            left.consumer_rank()
                .cmp(&right.consumer_rank())
                .then_with(|| left.matrix.cmp(&right.matrix))
        });
        matrices
    }

    fn consumer_rank(&self) -> usize {
        self.regions
            .iter()
            .flat_map(|region| region.main.consumers.iter())
            .filter_map(|consumer| {
                ALIAS_MAIN_CONSUMERS
                    .iter()
                    .position(|known_consumer| known_consumer == consumer)
            })
            .min()
            .unwrap_or(ALIAS_MAIN_CONSUMERS.len())
    }

    fn assert_definition(&self) {
        let mut axis_names = HashSet::new();
        for dimension in &self.local_dimensions {
            assert!(
                !dimension.name.trim().is_empty(),
                "alias matrix yaml {} local dimension has an empty name",
                self.matrix
            );
            assert!(
                axis_names.insert(dimension.name.as_str()),
                "alias matrix yaml {} local dimension is declared more than once: {}",
                self.matrix,
                dimension.name
            );
            assert!(
                !dimension.values.is_empty(),
                "alias matrix yaml {} local dimension {} must have values",
                self.matrix,
                dimension.name
            );
            let mut value_names = HashSet::new();
            assert!(
                dimension.values.len() <= 16,
                "alias matrix yaml {} local dimension {} cannot exceed sixteen values",
                self.matrix,
                dimension.name
            );
            for value in &dimension.values {
                assert!(
                    !value.trim().is_empty(),
                    "alias matrix yaml {} local dimension {} has an empty value",
                    self.matrix,
                    dimension.name
                );
                assert!(
                    value_names.insert(value.as_str()),
                    "alias matrix yaml {} local dimension {} declares duplicate value: {}",
                    self.matrix,
                    dimension.name,
                    value
                );
            }
        }
        for region in &self.regions {
            region.assert_definition(self);
        }
    }

    fn local_dimension_value_index(&self, axis: &str, value: &str) -> Option<usize> {
        let dimension = self
            .local_dimensions
            .iter()
            .find(|dimension| dimension.name == axis)?;
        dimension
            .values
            .iter()
            .position(|known_value| known_value == value)
    }

    fn has_local_dimension(&self, axis: &str) -> bool {
        self.local_dimensions
            .iter()
            .any(|dimension| dimension.name == axis)
    }

    fn has_local_dimension_value(&self, axis: &str, value: &str) -> bool {
        self.local_dimension_value_index(axis, value).is_some()
    }

    fn expected_keys(&self) -> HashSet<String> {
        assert!(
            !self.regions.is_empty(),
            "alias matrix yaml {} must declare matrix regions",
            self.matrix
        );

        let mut candidate_keys = HashSet::new();
        let mut expected = HashSet::new();
        for (region_index, region) in self.regions.iter().enumerate() {
            if region.excluded {
                continue;
            }
            let candidates = region.coordinates(self, region_index);
            for coordinate in &candidates {
                assert!(
                    candidate_keys.insert(coordinate.key.clone()),
                    "alias matrix yaml {} declares duplicate matrix coordinate: {}",
                    self.matrix,
                    coordinate.key
                );
            }
            expected.extend(region.unmasked_keys(self, candidates));
        }

        expected
    }

    async fn write_cases(&self, golden_dir: &str) -> Result<()> {
        let file_name = format!("{golden_dir}/{}.txt", self.matrix);
        let mut file = open_golden_file("semantic", &file_name)?;
        for region in &self.regions {
            for case in &region.cases {
                case.write_runs(&mut file, self).await?;
            }
        }
        Ok(())
    }

    fn encode_local_context(&self, local: &BTreeMap<String, String>) -> String {
        let mut context = String::new();
        for dimension in &self.local_dimensions {
            if let Some(value) = local.get(&dimension.name) {
                let value_index = self
                    .local_dimension_value_index(&dimension.name, value)
                    .unwrap_or_else(|| {
                        panic!(
                            "alias matrix yaml {} references unknown local value: {}.{}",
                            self.matrix, dimension.name, value
                        )
                    });
                context.push(
                    char::from_digit(value_index as u32, 16)
                        .expect("local dimension value index should fit in one hex digit")
                        .to_ascii_uppercase(),
                );
            }
        }
        context
    }
}

impl AliasMatrixRegion {
    fn assert_definition(&self, matrix: &AliasMatrixFile) {
        if self.excluded {
            assert!(
                self.reason
                    .as_deref()
                    .is_some_and(|reason| !reason.trim().is_empty()),
                "alias matrix yaml {} excluded region must explain why it has no cases",
                matrix.matrix
            );
            assert!(
                self.local.is_empty(),
                "alias matrix yaml {} excluded region must not select local dimensions",
                matrix.matrix
            );
            assert!(
                self.masks.is_empty(),
                "alias matrix yaml {} excluded region must not declare masks",
                matrix.matrix
            );
        } else {
            assert!(
                self.reason.is_none(),
                "alias matrix yaml {} non-excluded region must explain missing coordinates with masks",
                matrix.matrix
            );
        }

        for (axis, values) in &self.local {
            assert!(
                matrix.has_local_dimension(axis),
                "alias matrix yaml {} region references unknown local dimension: {}",
                matrix.matrix,
                axis
            );
            assert!(
                !values.is_empty(),
                "alias matrix yaml {} region local dimension {} has no selected values",
                matrix.matrix,
                axis
            );
            for value in values {
                assert!(
                    matrix.has_local_dimension_value(axis, value),
                    "alias matrix yaml {} region references unknown local value: {}.{}",
                    matrix.matrix,
                    axis,
                    value
                );
            }
        }

        for mask in &self.masks {
            mask.assert_definition(matrix);
        }
    }

    fn main_coordinates(&self, matrix: &AliasMatrixFile) -> Vec<String> {
        assert!(
            !self.description.trim().is_empty(),
            "alias matrix yaml {} has a region without a semantic description",
            matrix.matrix
        );
        self.main.assert_definition(&matrix.matrix);

        let mut coordinates = Vec::new();
        for consumer in &self.main.consumers {
            for name_shape in &self.main.name_shapes {
                for producer in &self.main.producers {
                    let main = format!("{consumer}{name_shape}{producer}");
                    assert_main_coordinate(&main, &matrix.matrix);
                    coordinates.push(main);
                }
            }
        }

        coordinates
    }

    fn coordinates(
        &self,
        matrix: &AliasMatrixFile,
        region_index: usize,
    ) -> Vec<AliasMatrixGeneratedCoordinate> {
        let local_coordinates = self.local_coordinates(matrix);
        let mut candidates = Vec::new();
        for main in self.main_coordinates(matrix) {
            let consumer = main[0..2].to_string();
            let name_shape = main[2..4].to_string();
            let producer = main[4..6].to_string();
            for local in &local_coordinates {
                let context = matrix.encode_local_context(local);
                let key = format!("{main}_{region_index}_{context}");
                candidates.push(AliasMatrixGeneratedCoordinate {
                    key,
                    consumer: consumer.clone(),
                    name_shape: name_shape.clone(),
                    producer: producer.clone(),
                    local: local.clone(),
                });
            }
        }

        candidates
    }

    fn local_coordinates(&self, matrix: &AliasMatrixFile) -> Vec<BTreeMap<String, String>> {
        let mut coordinates = vec![BTreeMap::new()];
        for (axis, values) in &self.local {
            assert!(
                matrix.has_local_dimension(axis),
                "alias matrix yaml {} region references unknown local dimension: {}",
                matrix.matrix,
                axis
            );
            let mut next = Vec::new();
            for coordinate in coordinates {
                for value in values {
                    assert!(
                        matrix.has_local_dimension_value(axis, value),
                        "alias matrix yaml {} region references unknown local value: {}.{}",
                        matrix.matrix,
                        axis,
                        value
                    );
                    let mut coordinate = coordinate.clone();
                    coordinate.insert(axis.clone(), value.clone());
                    next.push(coordinate);
                }
            }
            coordinates = next;
        }
        coordinates
    }

    fn expected_keys(&self, matrix: &AliasMatrixFile) -> HashSet<String> {
        if self.excluded {
            HashSet::new()
        } else {
            let region_index = matrix
                .regions
                .iter()
                .position(|region| std::ptr::eq(region, self))
                .expect("region should belong to matrix");
            let candidates = self.coordinates(matrix, region_index);
            self.unmasked_keys(matrix, candidates)
        }
    }

    fn unmasked_keys(
        &self,
        matrix: &AliasMatrixFile,
        candidates: Vec<AliasMatrixGeneratedCoordinate>,
    ) -> HashSet<String> {
        let mut masked = HashSet::new();
        for mask in &self.masks {
            let before = masked.len();
            for coordinate in &candidates {
                if mask.matches(coordinate) {
                    assert!(
                        masked.insert(coordinate.key.clone()),
                        "alias matrix yaml {} masks the same coordinate more than once: {}",
                        matrix.matrix,
                        coordinate.key
                    );
                }
            }
            assert!(
                masked.len() > before,
                "alias matrix yaml {} mask does not remove any candidate coordinate",
                matrix.matrix
            );
        }

        candidates
            .into_iter()
            .map(|coordinate| coordinate.key)
            .filter(|key| !masked.contains(key))
            .collect()
    }

    fn owns_excluded_case(
        &self,
        matrix: &AliasMatrixFile,
        region_index: usize,
        case: &AliasMatrixYamlCase,
    ) -> bool {
        self.main_coordinates(matrix)
            .contains(&case.main_coordinate(&matrix.matrix))
            && case.region_index(&matrix.matrix) == region_index
    }
}

impl AliasMatrixMainSelector {
    fn assert_definition(&self, matrix: &str) {
        assert!(
            !self.consumers.is_empty(),
            "alias matrix yaml {matrix} region main selector must choose consumers"
        );
        assert!(
            !self.name_shapes.is_empty(),
            "alias matrix yaml {matrix} region main selector must choose name shapes"
        );
        assert!(
            !self.producers.is_empty(),
            "alias matrix yaml {matrix} region main selector must choose producers"
        );
        for consumer in &self.consumers {
            assert_coordinate_segment(consumer, matrix, "consumer");
        }
        for name_shape in &self.name_shapes {
            assert_coordinate_segment(name_shape, matrix, "name shape");
        }
        for producer in &self.producers {
            assert_coordinate_segment(producer, matrix, "producer");
        }
    }
}

impl AliasMatrixMask {
    fn assert_definition(&self, matrix: &AliasMatrixFile) {
        assert!(
            !self.reason.trim().is_empty(),
            "alias matrix yaml {} mask must explain why it removes coordinates",
            matrix.matrix
        );
        self.when.assert_definition(matrix);
    }

    fn matches(&self, coordinate: &AliasMatrixGeneratedCoordinate) -> bool {
        self.when.matches(coordinate)
    }
}

impl AliasMatrixMaskSelector {
    fn assert_definition(&self, matrix: &AliasMatrixFile) {
        if let Some(consumers) = &self.main.consumers {
            assert!(
                !consumers.is_empty(),
                "alias matrix yaml {} mask consumer selector is empty",
                matrix.matrix
            );
            for consumer in consumers {
                assert_coordinate_segment(consumer, &matrix.matrix, "consumer");
            }
        }
        if let Some(name_shapes) = &self.main.name_shapes {
            assert!(
                !name_shapes.is_empty(),
                "alias matrix yaml {} mask name-shape selector is empty",
                matrix.matrix
            );
            for name_shape in name_shapes {
                assert_coordinate_segment(name_shape, &matrix.matrix, "name shape");
            }
        }
        if let Some(producers) = &self.main.producers {
            assert!(
                !producers.is_empty(),
                "alias matrix yaml {} mask producer selector is empty",
                matrix.matrix
            );
            for producer in producers {
                assert_coordinate_segment(producer, &matrix.matrix, "producer");
            }
        }

        assert!(
            self.main.consumers.is_some()
                || self.main.name_shapes.is_some()
                || self.main.producers.is_some()
                || !self.local.is_empty(),
            "alias matrix yaml {} mask selector must choose at least one dimension",
            matrix.matrix
        );

        for (axis, values) in &self.local {
            assert!(
                matrix.has_local_dimension(axis),
                "alias matrix yaml {} mask references unknown local dimension: {}",
                matrix.matrix,
                axis
            );
            assert!(
                !values.is_empty(),
                "alias matrix yaml {} mask local dimension {} has no selected values",
                matrix.matrix,
                axis
            );
            for value in values {
                assert!(
                    matrix.has_local_dimension_value(axis, value),
                    "alias matrix yaml {} mask references unknown local value: {}.{}",
                    matrix.matrix,
                    axis,
                    value
                );
            }
        }
    }

    fn matches(&self, coordinate: &AliasMatrixGeneratedCoordinate) -> bool {
        self.main
            .consumers
            .as_ref()
            .is_none_or(|values| values.contains(&coordinate.consumer))
            && self
                .main
                .name_shapes
                .as_ref()
                .is_none_or(|values| values.contains(&coordinate.name_shape))
            && self
                .main
                .producers
                .as_ref()
                .is_none_or(|values| values.contains(&coordinate.producer))
            && self.local.iter().all(|(axis, values)| {
                coordinate
                    .local
                    .get(axis)
                    .is_some_and(|value| values.contains(value))
            })
    }
}

impl AliasMatrixYamlCase {
    fn assert_runs(&self, matrix: &AliasMatrixFile) {
        assert!(
            !self.runs.is_empty(),
            "alias matrix yaml {} case {} must have at least one run",
            matrix.matrix,
            self.name
        );
        for run in &self.runs {
            assert!(
                !run.sql.trim().is_empty(),
                "alias matrix yaml {} case {} has an empty SQL run",
                matrix.matrix,
                self.name
            );
        }
    }

    fn assert_key(&self, matrix: &str) {
        let (main, region, context) = self.key_parts(matrix);
        assert_main_coordinate(main, matrix);
        assert_region_index(region, matrix);
        assert_hex_context(context, matrix);
    }

    fn main_coordinate(&self, matrix: &str) -> String {
        let (main, _, _) = self.key_parts(matrix);
        assert_main_coordinate(main, matrix);
        main.to_string()
    }

    fn region_index(&self, matrix: &str) -> usize {
        let (_, region, _) = self.key_parts(matrix);
        assert_region_index(region, matrix);
        region.parse::<usize>().unwrap_or_else(|err| {
            panic!(
                "alias matrix yaml {matrix} region index must be decimal digits: {region}: {err}"
            )
        })
    }

    fn key_parts<'a>(&'a self, matrix: &str) -> (&'a str, &'a str, &'a str) {
        let parts = self.key.split('_').collect::<Vec<_>>();
        assert!(
            parts.len() == 3,
            "alias matrix yaml {matrix} key must have main, region, and local parts separated by underscores: {}",
            self.key
        );
        (parts[0], parts[1], parts[2])
    }

    async fn write_runs(&self, file: &mut impl Write, matrix: &AliasMatrixFile) -> Result<()> {
        let ctx = LiteTableContext::create().await?;
        for setup_sql in &self.setup_sqls {
            ctx.register_setup_sql(setup_sql).await?;
        }
        let multi_run = self.runs.len() > 1;
        for (index, run) in self.runs.iter().enumerate() {
            ctx.get_settings().set_setting(
                "enable_group_by_column_first".to_string(),
                if run.settings.enable_group_by_column_first {
                    "1"
                } else {
                    "0"
                }
                .to_string(),
            )?;

            let run_name = if multi_run {
                format!(
                    "{}__{}__group_by_column_first_{}__run_{}",
                    matrix.matrix,
                    self.name,
                    if run.settings.enable_group_by_column_first {
                        "on"
                    } else {
                        "off"
                    },
                    index + 1,
                )
            } else {
                format!("{}__{}", matrix.matrix, self.name)
            };

            write_case_title(file, &run_name, "")?;
            writeln!(file, "matrix: {}", matrix.matrix)?;
            writeln!(file, "key: {}", self.key)?;
            writeln!(
                file,
                "setting: enable_group_by_column_first={}",
                if run.settings.enable_group_by_column_first {
                    1
                } else {
                    0
                }
            )?;
            writeln!(file, "sql: {}", run.sql)?;
            let outcome = match ctx.bind_sql(&run.sql).await {
                Ok(plan) => SqlTestOutcome::Plan(plan.format_indent(Default::default())?),
                Err(err) => SqlTestOutcome::Error {
                    code: err.code(),
                    message: err.message(),
                },
            };
            write_case_outcome(file, &outcome)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct AliasMatrixCoverage {
    matrix_names: HashSet<String>,
    case_names: HashSet<String>,
    main_coordinates: HashSet<String>,
    main_owners: BTreeMap<String, HashSet<String>>,
}

impl AliasMatrixCoverage {
    fn assert_matrices(&mut self, matrices: &[AliasMatrixFile]) {
        for matrix in matrices {
            self.assert_matrix(matrix);
        }
        self.assert_global_main_coverage();
    }

    fn assert_matrix(&mut self, matrix: &AliasMatrixFile) {
        assert!(
            self.matrix_names.insert(matrix.matrix.clone()),
            "duplicate alias matrix yaml: {}",
            matrix.matrix
        );
        matrix.assert_definition();

        let expected = matrix.expected_keys();
        let covered = self.assert_matrix_cases(matrix);
        let mut missing = expected.difference(&covered).collect::<Vec<&String>>();
        missing.sort();
        assert!(
            missing.is_empty(),
            "alias matrix yaml {} unmasked keys are not covered by cases: {missing:?}",
            matrix.matrix
        );
    }

    fn assert_matrix_cases(&mut self, matrix: &AliasMatrixFile) -> HashSet<String> {
        let mut covered = HashSet::new();
        for (region_index, region) in matrix.regions.iter().enumerate() {
            self.register_region(matrix, region);
            let region_expected = region.expected_keys(matrix);
            for case in &region.cases {
                self.assert_case(
                    matrix,
                    region,
                    region_index,
                    &region_expected,
                    case,
                    &mut covered,
                );
            }
        }
        covered
    }

    fn assert_case(
        &mut self,
        matrix: &AliasMatrixFile,
        region: &AliasMatrixRegion,
        region_index: usize,
        region_expected: &HashSet<String>,
        case: &AliasMatrixYamlCase,
        covered: &mut HashSet<String>,
    ) {
        case.assert_key(&matrix.matrix);
        if region.excluded {
            assert!(
                region.owns_excluded_case(matrix, region_index, case),
                "alias matrix yaml {} excluded-region case {} covers a main coordinate outside its region: {}",
                matrix.matrix,
                case.name,
                case.key
            );
        } else {
            assert!(
                region_expected.contains(case.key.as_str()),
                "alias matrix yaml {} case {} covers a key outside its region: {}",
                matrix.matrix,
                case.name,
                case.key
            );
        }
        assert!(
            covered.insert(case.key.clone()),
            "alias matrix yaml {} key is covered by more than one case: {}",
            matrix.matrix,
            case.key
        );
        assert!(
            self.case_names
                .insert(format!("{}/{}", matrix.matrix, case.name)),
            "duplicate alias matrix yaml case name: {}/{}",
            matrix.matrix,
            case.name
        );
        case.assert_runs(matrix);
    }

    fn register_region(&mut self, matrix: &AliasMatrixFile, region: &AliasMatrixRegion) {
        for main in region.main_coordinates(matrix) {
            self.main_coordinates.insert(main.clone());
            self.main_owners
                .entry(main)
                .or_default()
                .insert(matrix.matrix.clone());
        }
    }

    fn assert_global_main_coverage(&self) {
        let expected_main_coordinates = expected_alias_matrix_main_coordinates();
        let mut missing_main = expected_main_coordinates
            .difference(&self.main_coordinates)
            .collect::<Vec<&String>>();
        missing_main.sort();
        assert!(
            missing_main.is_empty(),
            "alias matrix yaml main coordinates are not covered by any region: {missing_main:?}"
        );

        let duplicate_main_owners = self
            .main_owners
            .iter()
            .filter(|(_, owners)| owners.len() > 1)
            .map(|(coordinate, owners)| {
                let mut owners = owners.iter().cloned().collect::<Vec<_>>();
                owners.sort();
                format!("{coordinate}: {owners:?}")
            })
            .collect::<Vec<_>>();
        assert!(
            duplicate_main_owners.is_empty(),
            "alias matrix yaml main coordinates are owned by multiple matrices: {duplicate_main_owners:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alias_resolution_matrix_yaml_cases() -> Result<()> {
    let matrices = AliasMatrixFile::load_all();
    AliasMatrixCoverage::default().assert_matrices(&matrices);
    let golden_dir = ALIAS_MATRIX_DIR
        .strip_prefix("tests/it/semantic/")
        .unwrap_or(ALIAS_MATRIX_DIR)
        .to_string();
    for matrix in &matrices {
        matrix.write_cases(&golden_dir).await?;
    }
    Ok(())
}

fn expected_alias_matrix_main_coordinates() -> HashSet<String> {
    let mut coordinates = HashSet::new();
    for consumer in ALIAS_MAIN_CONSUMERS {
        for name_shape in ALIAS_MAIN_NAME_SHAPES {
            for producer in ALIAS_MAIN_PRODUCERS {
                coordinates.insert(format!("{consumer}{name_shape}{producer}"));
            }
        }
    }
    coordinates
}

fn assert_main_coordinate(main: &str, matrix: &str) {
    assert!(
        main.len() == 6,
        "alias matrix yaml {matrix} main coordinate must be six characters: {main}"
    );
    for segment in main.as_bytes().chunks_exact(2) {
        let segment = std::str::from_utf8(segment).expect("main coordinate should be ASCII");
        assert_coordinate_segment(segment, matrix, "main coordinate");
    }
}

fn assert_coordinate_segment(segment: &str, matrix: &str, name: &str) {
    let bytes = segment.as_bytes();
    assert!(
        bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1].is_ascii_digit(),
        "alias matrix yaml {matrix} {name} segment must be uppercase-letter plus digit: {segment}"
    );
}

fn assert_region_index(region: &str, matrix: &str) {
    assert!(
        !region.is_empty() && region.bytes().all(|byte| byte.is_ascii_digit()),
        "alias matrix yaml {matrix} region index must be decimal digits: {region}"
    );
}

fn assert_hex_context(context: &str, matrix: &str) {
    assert!(
        context
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_lowercase()),
        "alias matrix yaml {matrix} local context must be empty or uppercase hex digits: {context}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alias_resolution_prepass_metadata() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "having_alias_and_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t HAVING s > 0 AND EXISTS (SELECT 1 FROM t AS inner_t WHERE inner_t.number > 0)",
        },
        SqlTestCase {
            name: "having_alias_to_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT (SELECT max(number) FROM t AS inner_t) AS s FROM t HAVING s > 0",
        },
        SqlTestCase {
            name: "order_by_alias_to_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT (SELECT max(number) FROM t AS inner_t) AS s FROM t ORDER BY s",
        },
    ];

    for case in cases {
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let Plan::Query { metadata, .. } = plan else {
            panic!("expected query plan for {}", case.name);
        };

        let table_count = metadata.read().tables().len();
        assert_eq!(
            table_count, 2,
            "{} should only keep metadata for the outer query and the final subquery bind",
            case.name
        );
    }

    Ok(())
}
