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

use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::number::NumberScalar;
use databend_query::data::search;
use databend_query::data::search::RetrieverSpec;
use databend_query::data::search::SearchOutput;
use databend_query::data::search::SearchRequest;
use databend_query::data::search::TextOperator;
use databend_query::data::search::TextSpec;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;

/// A row as (id, title, body, score).
type Row = (String, String, String, f32);

async fn sql_rows(fixture: &TestFixture, sql: &str) -> anyhow::Result<Vec<Row>> {
    let blocks = fixture
        .execute_query(sql)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let mut rows = Vec::new();
    for block in blocks {
        for i in 0..block.num_rows() {
            let value = |c: usize| block.get_by_offset(c).index(i).unwrap();
            rows.push((
                value(0).to_string(),
                value(1).to_string(),
                value(2).to_string(),
                score(value(3)),
            ));
        }
    }
    Ok(rows)
}

fn score(value: ScalarRef) -> f32 {
    match value {
        ScalarRef::Number(NumberScalar::Float32(score)) => score.0,
        other => panic!("score is not a Float32: {other:?}"),
    }
}

/// The rows of a search with output `[id, _score, body, title]`, as (id, title, body, score).
fn search_rows(output: &SearchOutput) -> Vec<Row> {
    let block = &output.block;
    let names = output
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["id", "_score", "body", "title"]);
    (0..block.num_rows())
        .map(|i| {
            let value = |c: usize| block.get_by_offset(c).index(i).unwrap();
            (
                value(0).to_string(),
                value(3).to_string(),
                value(2).to_string(),
                score(value(1)),
            )
        })
        .collect()
}

struct Case {
    columns: &'static [&'static str],
    query: &'static str,
    operator: TextOperator,
    lenient: bool,
}

impl Case {
    fn match_sql(&self) -> String {
        let mut options = Vec::new();
        if self.operator == TextOperator::And {
            options.push("operator=AND");
        }
        if self.lenient {
            options.push("lenient=true");
        }
        let options = if options.is_empty() {
            String::new()
        } else {
            format!(", '{}'", options.join(";"))
        };
        format!(
            "match('{}', '{}'{options})",
            self.columns.join(", "),
            self.query.replace('\'', "''")
        )
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_text_search_returns_the_rows_of_sql() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.docs (id INT, title STRING, body STRING, \
             INVERTED INDEX fts (title, body) tokenizer = 'english') \
             ROW_PER_BLOCK = 40 STORAGE_FORMAT = 'parquet'"
        ))
        .await?;
    // Several segments of several blocks. Bodies repeat, so many rows tie on score
    // within a block and across blocks.
    for segment in 0..3 {
        fixture
            .execute_command(&format!(
                "INSERT INTO {db}.docs SELECT number + {segment} * 100, \
                 concat('title ', (number % 7)::STRING, if(number % 5 = 0, ' rust', '')), \
                 concat(repeat('alpha ', number % 4 + 1), repeat('beta ', number % 3), \
                        if(number % 2 = 0, 'gamma delta', 'delta')) \
                 FROM numbers(100)"
            ))
            .await?;
    }

    let cases = [
        Case {
            columns: &["body"],
            query: "alpha",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["body"],
            query: "alpha beta gamma",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["body"],
            query: "beta gamma",
            operator: TextOperator::And,
            lenient: false,
        },
        Case {
            columns: &["body"],
            query: "\"gamma delta\"",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["title^3", "body"],
            query: "rust OR gamma",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["title"],
            query: "rust",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["body"],
            query: "absent",
            operator: TextOperator::Or,
            lenient: false,
        },
        Case {
            columns: &["body"],
            query: "alpha AND (",
            operator: TextOperator::Or,
            lenient: true,
        },
    ];
    for case in &cases {
        let matched = case.match_sql();
        let all = sql_rows(
            &fixture,
            &format!(
                "SELECT id, title, body, score() FROM {db}.docs WHERE {matched} ORDER BY score() DESC"
            ),
        )
        .await?;
        let by_id = all
            .iter()
            .map(|row| (row.0.clone(), row.clone()))
            .collect::<HashMap<_, _>>();
        for (offset, limit) in [(0, 1), (0, 10), (5, 10), (0, 1000), (290, 20)] {
            let sql = sql_rows(
                &fixture,
                &format!(
                    "SELECT id, title, body, score() FROM {db}.docs WHERE {matched} \
                     ORDER BY score() DESC LIMIT {limit} OFFSET {offset}"
                ),
            )
            .await?;
            let request = SearchRequest {
                catalog: None,
                database: Some(db.clone()),
                table: "docs".to_string(),
                retrieve: RetrieverSpec::Text(TextSpec {
                    columns: case.columns.iter().map(|c| c.to_string()).collect(),
                    query: case.query.to_string(),
                    operator: case.operator,
                    fuzziness: None,
                    lenient: case.lenient,
                }),
                limit,
                offset,
                output: ["id", "_score", "body", "title"].map(String::from).to_vec(),
            };
            let output = search::execute(fixture.new_query_ctx().await?, request).await?;
            let found = search_rows(&output);
            let context = format!("{matched} offset={offset} limit={limit}");

            // The same ranks: SQL orders equal scores arbitrarily, so the scores of each
            // rank must be equal, and each row must be a matched row with its own values.
            let scores = |rows: &[Row]| rows.iter().map(|row| row.3).collect::<Vec<_>>();
            assert_eq!(scores(&found), scores(&sql), "{context}");
            for row in &found {
                assert_eq!(by_id.get(&row.0), Some(row), "{context}");
            }
            let ids = found.iter().map(|row| &row.0).collect::<HashSet<_>>();
            assert_eq!(ids.len(), found.len(), "{context}");
            assert_eq!(output.stats.rows, found.len(), "{context}");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_text_search_output_and_errors() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.docs (id INT, body STRING, note STRING, \
             INVERTED INDEX fts (body) tokenizer = 'english')"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.docs VALUES (1, 'alpha beta', 'x'), (2, 'alpha', 'y'), (3, 'beta', 'z')"
        ))
        .await?;
    let request = |columns: &[&str], output: &[&str]| SearchRequest {
        catalog: None,
        database: Some(db.clone()),
        table: "docs".to_string(),
        retrieve: RetrieverSpec::Text(TextSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            query: "alpha".to_string(),
            operator: TextOperator::Or,
            fuzziness: None,
            lenient: false,
        }),
        limit: 10,
        offset: 0,
        output: output.iter().map(|c| c.to_string()).collect(),
    };

    // Only the score, and columns in the requested order.
    let output = search::execute(
        fixture.new_query_ctx().await?,
        request(&["body"], &["_score"]),
    )
    .await?;
    assert_eq!(output.block.num_rows(), 2);
    assert_eq!(output.block.num_columns(), 1);
    let output = search::execute(
        fixture.new_query_ctx().await?,
        request(&["body"], &["note", "id"]),
    )
    .await?;
    let names = output
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["note", "id"]);
    assert_eq!(output.block.num_rows(), 2);

    let error = |code: u16, columns: &[&str], output: &[&str]| {
        let request = request(columns, output);
        let fixture = &fixture;
        async move {
            let error = search::execute(fixture.new_query_ctx().await?, request)
                .await
                .err()
                .expect("search must fail");
            assert_eq!(error.code(), code, "{error}");
            anyhow::Ok(())
        }
    };
    // Unindexed column, unknown search column, unknown output column.
    error(ErrorCode::BAD_ARGUMENTS, &["note"], &["id"]).await?;
    error(ErrorCode::UNKNOWN_COLUMN, &["missing"], &["id"]).await?;
    error(ErrorCode::UNKNOWN_COLUMN, &["body"], &["missing"]).await?;

    let mut missing = request(&["body"], &["id"]);
    missing.table = "missing".to_string();
    let error = search::execute(fixture.new_query_ctx().await?, missing)
        .await
        .err()
        .expect("search must fail");
    assert_eq!(error.code(), ErrorCode::UNKNOWN_TABLE, "{error}");
    Ok(())
}
