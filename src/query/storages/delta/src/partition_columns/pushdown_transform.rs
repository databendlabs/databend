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

use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;

pub fn get_pushdown_without_partition_columns(
    mut pushdown: PushDownInfo,
    partition_columns: &[FieldIndex],
) -> Result<PushDownInfo> {
    if partition_columns.is_empty() {
        return Ok(pushdown);
    }
    if let Some(ref mut p) = &mut pushdown.projection {
        *p = shift_projection(p.clone(), partition_columns)?;
    }
    if let Some(ref mut p) = &mut pushdown.output_columns {
        *p = shift_projection(p.clone(), partition_columns)?;
    }
    if let Some(ref mut p) = &mut pushdown.prewhere {
        p.output_columns = shift_projection(p.output_columns.clone(), partition_columns)?;
        p.prewhere_columns = shift_projection(p.prewhere_columns.clone(), partition_columns)?;
        p.remain_columns = shift_projection(p.remain_columns.clone(), partition_columns)?;
    }
    Ok(pushdown)
}

fn shift_projection_index(
    field_index: FieldIndex,
    partition_columns: &[FieldIndex],
) -> Option<Result<FieldIndex>> {
    let mut sub = 0;
    // most of the time, there a few partition columns, so let`s keep it simple.
    for col in partition_columns {
        if field_index == *col {
            return None;
        }
        if field_index > *col {
            sub += 1;
        }
    }
    if field_index < sub {
        Some(Err(ErrorCode::BadArguments(format!(
            "bug: field_index: {}, partition_columns: {:?}",
            field_index, partition_columns
        ))))
    } else {
        Some(Ok(field_index - sub))
    }
}

fn shift_projection(prj: Projection, partition_columns: &[FieldIndex]) -> Result<Projection> {
    let f = |prc| shift_projection_index(prc, partition_columns);
    match prj {
        Projection::Columns(columns) => {
            let res: Result<Vec<FieldIndex>> = columns.iter().filter_map(|i| f(*i)).collect();
            Ok(Projection::Columns(res?))
        }
        Projection::InnerColumns(map) => {
            let mut res = BTreeMap::new();
            for (i, mut columns) in map.into_iter() {
                if let Some(c0) = f(columns[0]) {
                    columns[0] = c0?;
                    res.insert(i, columns);
                }
            }
            Ok(Projection::InnerColumns(res))
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::Projection;

    use super::shift_projection;
    use super::shift_projection_index;

    #[test]
    fn test_shift_projection_index() {
        assert_eq!(
            shift_projection_index(0, &[0, 2]).transpose().unwrap(),
            None
        );
        assert_eq!(
            shift_projection_index(1, &[0, 2]).transpose().unwrap(),
            Some(0)
        );
        assert_eq!(
            shift_projection_index(2, &[0, 2]).transpose().unwrap(),
            None
        );
        assert_eq!(
            shift_projection_index(3, &[0, 2]).transpose().unwrap(),
            Some(1)
        );

        assert_eq!(
            shift_projection_index(0, &[1, 3]).transpose().unwrap(),
            Some(0)
        );
        assert_eq!(
            shift_projection_index(2, &[1, 3]).transpose().unwrap(),
            Some(1)
        );
        assert_eq!(
            shift_projection_index(4, &[1, 3]).transpose().unwrap(),
            Some(2)
        );
    }

    #[test]
    fn test_shift_projection() {
        assert_eq!(
            shift_projection(Projection::Columns(vec![0, 1, 2, 3, 4]), &[0, 2]).unwrap(),
            Projection::Columns(vec![0, 1, 2])
        );

        assert_eq!(
            shift_projection(
                Projection::InnerColumns(maplit::btreemap! {8 => vec![0, 1], 9 => vec![1, 2]}),
                &[0, 2]
            )
            .unwrap(),
            Projection::InnerColumns(maplit::btreemap! { 9 => vec![0, 2]})
        );
    }
}
