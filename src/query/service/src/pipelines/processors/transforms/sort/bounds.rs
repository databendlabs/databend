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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::sort::LoserTreeMerger;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::sort::SortedStream;

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct Bounds(
    // stored in reverse order of Column.
    Vec<Column>,
);

impl Bounds {
    pub fn from_column<R: Rows>(column: Column) -> Result<Bounds> {
        let block = DataBlock::sort(
            &DataBlock::new_from_columns(vec![column]),
            &[SortColumnDescription {
                offset: 0,
                asc: R::IS_ASC_COLUMN,
                nulls_first: false,
            }],
            None,
        )?;

        Ok(Bounds(vec![block.get_last_column().clone()]))
    }

    pub fn merge<R: Rows>(mut vector: Vec<Bounds>, batch_rows: usize) -> Result<Self> {
        match vector.len() {
            0 => Ok(Bounds(vec![])),
            1 => Ok(vector.pop().unwrap()),
            _ => {
                let schema = DataSchema::new(vec![DataField::new("order_col", R::data_type())]);
                let mut merger =
                    LoserTreeMerger::<R, _>::create(schema.into(), vector, batch_rows, None);

                let mut blocks = Vec::new();
                while let Some(block) = merger.next_block()? {
                    blocks.push(block)
                }
                debug_assert!(merger.is_finished());

                Ok(Bounds(
                    blocks
                        .iter()
                        .rev()
                        .map(|b| b.get_last_column().clone())
                        .collect(),
                ))
            }
        }
    }

    pub fn next_bound(&mut self) -> Option<Column> {
        let last = self.0.last_mut()?;
        match last.len() {
            0 => unreachable!(),
            1 => Some(self.0.pop().unwrap()),
            _ => {
                let bound = last.slice(0..1).maybe_gc();
                *last = last.slice(1..last.len());
                Some(bound)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(Column::len).sum()
    }

    #[expect(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|col| col.len() == 0)
    }

    pub fn reduce(&self, n: usize, data_type: DataType) -> Option<Self> {
        if n == 0 {
            return Some(Self::default());
        }
        let count = self.len();
        if n >= count {
            return None;
        }

        let step = count / n;
        let offset = step / 2;
        let indices = self
            .0
            .iter()
            .enumerate()
            .rev()
            .flat_map(|(b_idx, col)| std::iter::repeat_n(b_idx, col.len()).zip(0..col.len()))
            .enumerate()
            .take(step * n)
            .filter_map(|(i, (block, row))| {
                if i % step == offset {
                    Some((block as u32, row as u32, 1))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        Some(Bounds(vec![Column::take_column_indices(
            &self.0,
            data_type,
            &indices,
            indices.len(),
        )]))
    }
}

impl SortedStream for Bounds {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        match self.0.pop() {
            Some(column) => Ok((
                Some((DataBlock::new_from_columns(vec![column.clone()]), column)),
                false,
            )),
            None => Ok((None, false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::FromData;
    use databend_common_pipeline_transforms::sort::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sort::SimpleRowsDesc;

    use super::*;

    #[test]
    fn test_merge() -> Result<()> {
        {
            let column = Int32Type::from_data(vec![0, 7, 6, 6, 6]);
            let bounds = Bounds::from_column::<SimpleRowsAsc<Int32Type>>(column)?;
            assert_eq!(
                bounds,
                Bounds(vec![Int32Type::from_data(vec![0, 6, 6, 6, 7])])
            );

            let vector = vec![
                bounds,
                Bounds::default(),
                Bounds::from_column::<SimpleRowsAsc<Int32Type>>(Int32Type::from_data(vec![
                    0, 1, 2,
                ]))
                .unwrap(),
            ];
            let bounds = Bounds::merge::<SimpleRowsAsc<Int32Type>>(vector, 3)?;

            assert_eq!(
                bounds,
                Bounds(vec![
                    Int32Type::from_data(vec![6, 7]),
                    Int32Type::from_data(vec![2, 6, 6]),
                    Int32Type::from_data(vec![0, 0, 1]),
                ])
            );
        }

        {
            let data = vec![vec![77, -2, 7], vec![3, 8, 6, 1, 1], vec![2]];

            let data = data
                .into_iter()
                .map(|v| Bounds::from_column::<SimpleRowsDesc<Int32Type>>(Int32Type::from_data(v)))
                .collect::<Result<Vec<_>>>()?;
            let bounds = Bounds::merge::<SimpleRowsDesc<Int32Type>>(data, 2)?;

            assert_eq!(
                bounds,
                Bounds(vec![
                    Int32Type::from_data(vec![-2]),
                    Int32Type::from_data(vec![1, 1]),
                    Int32Type::from_data(vec![3, 2]),
                    Int32Type::from_data(vec![7, 6]),
                    Int32Type::from_data(vec![77, 8]),
                ])
            );
        }

        Ok(())
    }

    #[test]
    fn test_reduce() -> Result<()> {
        let data = vec![vec![77, -2, 7], vec![3, 8, 6, 1, 1], vec![2]];

        let data = data
            .into_iter()
            .map(|v| Bounds::from_column::<SimpleRowsDesc<Int32Type>>(Int32Type::from_data(v)))
            .collect::<Result<Vec<_>>>()?;
        let bounds = Bounds::merge::<SimpleRowsDesc<Int32Type>>(data, 2)?;

        let got = bounds.reduce(4, Int32Type::data_type()).unwrap();
        assert_eq!(got, Bounds(vec![Int32Type::from_data(vec![8, 6, 2, 1])])); // 77 _8 7 _6 3 _2 1 _1 -2

        let got = bounds.reduce(3, Int32Type::data_type()).unwrap();
        assert_eq!(got, Bounds(vec![Int32Type::from_data(vec![8, 3, 1])])); // 77 _8 7 6 _3 2 1 _1 -2

        let got = bounds.reduce(2, Int32Type::data_type()).unwrap();
        assert_eq!(got, Bounds(vec![Int32Type::from_data(vec![7, 1])])); // 77 8 _7 6 3 2 _1 1 -2

        let got = bounds.reduce(1, Int32Type::data_type()).unwrap();
        assert_eq!(got, Bounds(vec![Int32Type::from_data(vec![3])])); // 77 8 7 6 _3 2 1 1 -2

        Ok(())
    }
}
