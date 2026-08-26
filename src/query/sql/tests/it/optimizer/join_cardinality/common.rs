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

use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_sql::ColumnEntry;
use databend_common_sql::Metadata;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
use databend_common_statistics::Histogram;

fn column_label(metadata: &Metadata, column: Symbol) -> String {
    let id = column.as_usize();
    match metadata.column(column) {
        ColumnEntry::BaseTableColumn(column) => {
            let table = metadata.table(column.table_index);
            format!("{}.{} (#{id})", table.name(), column.column_name)
        }
        entry => format!("{} (#{id})", entry.name()),
    }
}

fn histogram_summary(histogram: &Histogram) -> String {
    let buckets = histogram
        .bucket_iter()
        .map(|bucket| {
            format!(
                "{}..{}:{:.3}/{:.3}",
                bucket.lower_bound(),
                bucket.upper_bound(),
                bucket.num_values(),
                bucket.num_distinct()
            )
        })
        .collect::<Vec<_>>();
    let bucket_summary = if buckets.len() <= 12 {
        format!("buckets=[{}]", buckets.join(", "))
    } else {
        let first = buckets[..3].join(", ");
        let last = buckets[buckets.len() - 3..].join(", ");
        format!(
            "bucket_count={}, buckets=[{}, ..., {}]",
            buckets.len(),
            first,
            last
        )
    };
    format!(
        "rows={:.3}, ndv={:.3}, {}",
        histogram.num_values(),
        histogram.ndv().expected.unwrap_or(histogram.ndv().upper),
        bucket_summary
    )
}

fn estimate_summary(expected: Option<f64>, upper: f64) -> String {
    match expected {
        Some(expected) if (expected - upper).abs() < 1e-9 => format!("{expected:.3}"),
        Some(expected) => format!("{expected:.3} (upper={upper:.3})"),
        None => format!("unknown (upper={upper:.3})"),
    }
}

fn count_summary(count: StatCount) -> String {
    estimate_summary(Some(count.expected()), count.upper())
}

pub(super) fn column_stat_summary(stat: &ColumnStat) -> String {
    match stat.bounds() {
        Some(bounds) => {
            let (min, max) = bounds.display_parts();
            format!(
                "min={}, max={}, ndv={}, null={}, histogram={}",
                min,
                max,
                estimate_summary(stat.ndv().expected, stat.ndv().upper),
                count_summary(stat.null_count()),
                match stat {
                    ColumnStat::Int { histogram, .. } => histogram
                        .as_ref()
                        .map(|histogram| Histogram::Int(histogram.clone())),
                    ColumnStat::UInt { histogram, .. } => histogram
                        .as_ref()
                        .map(|histogram| Histogram::UInt(histogram.clone())),
                    ColumnStat::Float { histogram, .. } => histogram
                        .as_ref()
                        .map(|histogram| Histogram::Float(histogram.clone())),
                    ColumnStat::Bytes { histogram, .. } => histogram
                        .as_ref()
                        .map(|histogram| Histogram::Bytes(histogram.clone())),
                    ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => None,
                }
                .as_ref()
                .map(histogram_summary)
                .unwrap_or_else(|| "none".to_string())
            )
        }
        None => {
            format!(
                "all_null=true, null={}, histogram=none",
                count_summary(stat.null_count())
            )
        }
    }
}

pub(super) fn write_join_stat_info(
    file: &mut impl Write,
    metadata: &Metadata,
    stat_info: &StatInfo,
) -> Result<()> {
    let mut column_stats = stat_info.statistics.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(column, _)| **column);

    for (column, stat) in column_stats {
        writeln!(
            file,
            "stat          : {} {}",
            column_label(metadata, *column),
            column_stat_summary(stat)
        )?;
    }

    Ok(())
}

pub(super) fn collect_join_cardinalities(
    file: &mut impl Write,
    metadata: &Metadata,
    expr: &SExpr,
    expected_join_type: JoinType,
    case_name: &str,
) -> Result<usize> {
    let mut joins = 0;
    if let RelOperator::Join(join) = expr.plan() {
        assert_eq!(
            join.join_type, expected_join_type,
            "unexpected join type for {case_name}"
        );
        let stat_info = RelExpr::with_s_expr(expr).derive_cardinality()?;
        writeln!(
            file,
            "join          : {:<11} cardinality={:.3}",
            join.join_type, stat_info.cardinality
        )?;
        write_join_stat_info(file, metadata, &stat_info)?;
        joins += 1;
    }

    for child in expr.children() {
        joins += collect_join_cardinalities(file, metadata, child, expected_join_type, case_name)?;
    }

    Ok(joins)
}
