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
use std::collections::BTreeSet;

use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::number::NumberScalar;
use databend_common_storages_fuse::io::FuseCaches;
use databend_query::data::search;
use databend_query::data::search::RetrieverSpec;
use databend_query::data::search::SearchRequest;
use databend_query::data::search::VectorDistance;
use databend_query::data::search::VectorSpec;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::OSSSetup;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;

const DIMENSION: usize = 8;

/// Deterministic pseudo-random vectors in [-1, 1), distinct. Their SQ8 scores may still
/// tie (see [`assert_same_ranks`]).
fn vectors(seed: u64, n: usize) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            (0..DIMENSION)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect()
        })
        .collect()
}

fn literal(vector: &[f32]) -> String {
    let values = vector.iter().map(f32::to_string).collect::<Vec<_>>();
    format!("[{}]::VECTOR({DIMENSION})", values.join(", "))
}

/// Rows as (id, distance).
fn rows(blocks: &[DataBlock]) -> Vec<(String, f32)> {
    let mut rows = Vec::new();
    for block in blocks {
        for i in 0..block.num_rows() {
            let id = block.get_by_offset(0).index(i).unwrap().to_string();
            let ScalarRef::Number(NumberScalar::Float32(distance)) =
                block.get_by_offset(1).index(i).unwrap()
            else {
                panic!("distance is not a Float32");
            };
            rows.push((id, distance.0));
        }
    }
    rows
}

/// `got` ranks the rows of `want`: the same distance at every rank, and the same rows at
/// each distance but the last, of which SQL returns any. SQL orders rows of equal distance
/// arbitrarily, and the SQ8 distances of distinct vectors can be equal.
fn assert_same_ranks(got: &[(String, f32)], want: &[(String, f32)], context: &str) {
    let distances = |rows: &[(String, f32)]| rows.iter().map(|r| r.1).collect::<Vec<_>>();
    assert_eq!(distances(got), distances(want), "{context}");
    let Some((_, last)) = want.last() else {
        return;
    };
    let groups = |rows: &[(String, f32)]| {
        let mut groups: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
        for (id, distance) in rows.iter().filter(|r| r.1 != *last) {
            let new = groups
                .entry(distance.to_bits())
                .or_default()
                .insert(id.clone());
            assert!(new, "{context}: row {id} twice");
        }
        groups
    };
    assert_eq!(groups(got), groups(want), "{context}");
    let ids = got.iter().map(|r| &r.0).collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), got.len(), "{context}: duplicate rows");
}

async fn create_table(fixture: &TestFixture, name: &str, index: &str) -> anyhow::Result<()> {
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.{name} (id INT, v VECTOR({DIMENSION}), \
             VECTOR INDEX idx (v) {index}) ROW_PER_BLOCK = 100"
        ))
        .await?;
    // Several segments of several blocks.
    for (segment, chunk) in vectors(7, 600).chunks(200).enumerate() {
        let values = chunk
            .iter()
            .enumerate()
            .map(|(i, v)| format!("({}, {})", segment * 200 + i, literal(v)))
            .collect::<Vec<_>>();
        fixture
            .execute_command(&format!(
                "INSERT INTO {db}.{name} VALUES {}",
                values.join(", ")
            ))
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vector_search_returns_the_rows_of_sql() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    create_table(
        &fixture,
        "refined",
        "distance = 'cosine,l1,l2' refine = true",
    )
    .await?;
    create_table(&fixture, "plain", "distance = 'l2'").await?;

    let cases = [
        ("refined", VectorDistance::Cosine),
        ("refined", VectorDistance::L1),
        ("refined", VectorDistance::L2),
        ("plain", VectorDistance::L2),
    ];
    // The default factor refines the candidates; 0 ranks by the SQ8 scores of HNSW.
    for refine_factor in ["2", "0"] {
        fixture.default_session().get_settings().set_setting(
            "vector_refine_factor".to_string(),
            refine_factor.to_string(),
        )?;
        for (table, distance) in cases {
            for query in vectors(11, 4) {
                for (offset, limit) in [(0, 1), (0, 10), (5, 10), (0, 100)] {
                    let sql = format!(
                        "SELECT id, {}(v, {}) AS d FROM {db}.{table} \
                         ORDER BY d LIMIT {limit} OFFSET {offset}",
                        distance.func_name(),
                        literal(&query)
                    );
                    let want = rows(
                        &fixture
                            .execute_query(&sql)
                            .await?
                            .try_collect::<Vec<_>>()
                            .await?,
                    );
                    let request = SearchRequest {
                        catalog: None,
                        database: Some(db.clone()),
                        table: table.to_string(),
                        retrieve: RetrieverSpec::Vector(VectorSpec {
                            column: "v".to_string(),
                            query: query.clone(),
                            distance,
                        }),
                        limit,
                        offset,
                        output: vec!["id".to_string(), "_score".to_string()],
                    };
                    let output = search::execute(fixture.new_query_ctx().await?, request).await?;
                    let got = rows(&[output.block]);
                    let context = format!("{sql}, vector_refine_factor = {refine_factor}");
                    assert_same_ranks(&got, &want, &context);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vector_search_errors() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    create_table(&fixture, "t", "distance = 'l2'").await?;
    let error = |column: &str, query: Vec<f32>, distance| {
        let request = SearchRequest {
            catalog: None,
            database: Some(db.clone()),
            table: "t".to_string(),
            retrieve: RetrieverSpec::Vector(VectorSpec {
                column: column.to_string(),
                query,
                distance,
            }),
            limit: 10,
            offset: 0,
            output: vec!["id".to_string()],
        };
        let fixture = &fixture;
        async move {
            let ctx = fixture.new_query_ctx().await?;
            let error = search::execute(ctx, request)
                .await
                .err()
                .expect("search must fail");
            anyhow::Ok(error)
        }
    };
    let v = vec![0.5; DIMENSION];
    // No index of the distance, not a vector column, unknown column, wrong dimension.
    for (column, query, distance, code) in [
        (
            "v",
            v.clone(),
            VectorDistance::Cosine,
            ErrorCode::BAD_ARGUMENTS,
        ),
        (
            "id",
            v.clone(),
            VectorDistance::L2,
            ErrorCode::BAD_ARGUMENTS,
        ),
        (
            "missing",
            v.clone(),
            VectorDistance::L2,
            ErrorCode::UNKNOWN_COLUMN,
        ),
        (
            "v",
            vec![0.5; 3],
            VectorDistance::L2,
            ErrorCode::BAD_ARGUMENTS,
        ),
    ] {
        let e = error(column, query, distance).await?;
        assert_eq!(e.code(), code, "{column}: {e}");
    }
    Ok(())
}

/// The output columns of a search are decoded whole and cached like a SQL row fetch, so
/// repeated searches reuse them: decoding only the selected rows bypasses the cache, and
/// decoded every column chunk again on every search.
#[tokio::test(flavor = "multi_thread")]
async fn test_search_caches_fetched_columns() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.cache.table_data_deserialized_data_bytes = 64 << 20;
    let fixture = TestFixture::setup_with_custom(OSSSetup { config }).await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    create_table(&fixture, "t", "distance = 'l2'").await?;

    let request = SearchRequest {
        catalog: None,
        database: Some(db.clone()),
        table: "t".to_string(),
        retrieve: RetrieverSpec::Vector(VectorSpec {
            column: "v".to_string(),
            query: vectors(13, 1).remove(0),
            distance: VectorDistance::L2,
        }),
        limit: 50,
        offset: 0,
        output: vec!["id".to_string(), "_score".to_string()],
    };
    let cached = || FuseCaches::instance().column_arrays.stats().items;
    let before = cached();
    let first = search::execute(fixture.new_query_ctx().await?, request.clone()).await?;
    let after_first = cached();
    assert!(
        after_first > before,
        "the search cached no decoded column: {before} -> {after_first}"
    );
    let second = search::execute(fixture.new_query_ctx().await?, request).await?;
    assert_eq!(cached(), after_first, "the second search decoded again");
    assert_eq!(rows(&[second.block]), rows(&[first.block]));
    Ok(())
}
