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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;

#[derive(Default)]
pub struct DataSummary {
    pub row_counts: usize,
    pub input_bytes: usize,
    pub output_bytes: usize,
}

impl DataSummary {
    pub fn new() -> Self {
        DataSummary {
            row_counts: 0,
            input_bytes: 0,
            output_bytes: 0,
        }
    }

    pub fn add(&mut self, other: &Self) {
        self.row_counts += other.row_counts;
        self.input_bytes += other.input_bytes;
        self.output_bytes += other.output_bytes;
    }

    pub fn to_block(&self) -> DataBlock {
        let entries = vec![
            BlockEntry::new(
                DataType::Number(NumberDataType::UInt64),
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(self.row_counts as u64))),
            ),
            BlockEntry::new(
                DataType::Number(NumberDataType::UInt64),
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                    self.input_bytes as u64,
                ))),
            ),
            BlockEntry::new(
                DataType::Number(NumberDataType::UInt64),
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                    self.output_bytes as u64,
                ))),
            ),
        ];
        DataBlock::new(entries, 1)
    }

    pub fn from_block(block: &DataBlock) -> Self {
        assert_eq!(block.num_rows(), 1);
        assert_eq!(block.num_columns(), 3);
        let values = &block
            .columns()
            .iter()
            .map(|x| match x.value {
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(n))) => n,
                _ => {
                    unreachable!()
                }
            })
            .collect::<Vec<u64>>();
        DataSummary {
            row_counts: values[0] as usize,
            input_bytes: values[1] as usize,
            output_bytes: values[2] as usize,
        }
    }
}

pub enum UnloadOutput {
    Summary(DataSummary),
    Detail(Vec<OutputFileInfo>),
}

pub struct OutputFileInfo {
    file_name: String,
    summary: DataSummary,
}

impl UnloadOutput {
    pub fn create(detailed_output: bool) -> Self {
        if detailed_output {
            UnloadOutput::Detail(vec![])
        } else {
            UnloadOutput::Summary(DataSummary::new())
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            UnloadOutput::Summary(s) => s.row_counts == 0,
            UnloadOutput::Detail(v) => v.is_empty(),
        }
    }

    pub fn add_file(&mut self, file_name: &str, summary: DataSummary) {
        match self {
            UnloadOutput::Summary(s) => {
                s.add(&summary);
            }
            UnloadOutput::Detail(files) => {
                files.push(OutputFileInfo {
                    file_name: file_name.to_string(),
                    summary,
                });
            }
        }
    }

    pub fn to_block_partial(&self) -> Vec<DataBlock> {
        match self {
            UnloadOutput::Summary(summary) => {
                vec![summary.to_block()]
            }
            UnloadOutput::Detail(files) => {
                let batch = 1000;
                let mut blocks = vec![];
                for i in 0..(files.len() + batch - 1) / batch {
                    let end = files.len().min((i + 1) * batch);
                    let chunk = &files[i * batch..end];
                    blocks.push(file_infos_to_block(chunk));
                }
                blocks
            }
        }
    }
}

fn file_infos_to_block(files: &[OutputFileInfo]) -> DataBlock {
    let mut paths = Vec::with_capacity(files.len());
    let mut rows = Vec::with_capacity(files.len());
    let mut sizes = Vec::with_capacity(files.len());
    for file in files {
        paths.push(file.file_name.clone().as_bytes().to_vec());
        rows.push(file.summary.row_counts as u64);
        sizes.push(file.summary.output_bytes as u64);
    }
    DataBlock::new_from_columns(vec![
        StringType::from_data(paths),
        UInt64Type::from_data(sizes),
        UInt64Type::from_data(rows),
    ])
}

#[derive(Default)]
pub struct SumSummaryTransform {
    summary: DataSummary,
}
impl AccumulatingTransform for SumSummaryTransform {
    const NAME: &'static str = "SumSummaryTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let summary = DataSummary::from_block(&block);
        self.summary.add(&summary);
        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if generate_data {
            Ok(vec![self.summary.to_block()])
        } else {
            Ok(vec![])
        }
    }

    fn interrupt(&self) {}
}
