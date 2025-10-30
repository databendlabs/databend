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

use std::any::Any;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFileInfo;
use databend_storages_common_stage::SingleFilePartition;
use opendal::Operator;

use crate::read::row_based::formats::CsvInputFormat;

#[async_backtrace::framed]
pub async fn csv_read_partitions(
    stage_table_info: &StageTableInfo,
    ctx: Arc<dyn TableContext>,
) -> Result<(PartStatistics, Partitions)> {
    let fmt = match &stage_table_info.stage_info.file_format_params {
        FileFormatParams::Csv(fmt) => fmt,
        _ => unreachable!("do_read_partitions expect csv"),
    };

    let thread_num = ctx.get_settings().get_max_threads()? as usize;

    let files = if let Some(files) = &stage_table_info.files_to_copy {
        files.clone()
    } else {
        stage_table_info.list_files(thread_num, None).await?
    };

    if matches!(fmt.compression, StageFileCompression::None) {
        let op = init_stage_operator(&stage_table_info.stage_info)?;

        let read_bytes = files.iter().map(|f| f.size as usize).sum();
        let read_rows = std::cmp::max(read_bytes / (stage_table_info.schema.fields.len() + 1), 1);

        let mut partitions = Vec::new();
        for file in files {
            match split_file(&op, &file, fmt, 1024 * 1024 * 512, 1024 * 2).await? {
                None => {
                    if file.size > 0 {
                        let part = SingleFilePartition {
                            path: file.path,
                            size: file.size as _,
                        };
                        partitions.push(Arc::new(Box::new(part) as Box<dyn PartInfo>));
                    }
                }
                Some(splits) => {
                    partitions.extend(
                        splits
                            .into_iter()
                            .map(|x| Arc::new(Box::new(x) as Box<dyn PartInfo>)),
                    );
                }
            };
        }

        let statistics = PartStatistics {
            snapshot: None,
            read_rows,
            read_bytes,
            partitions_scanned: partitions.len(),
            partitions_total: partitions.len(),
            is_exact: false,
            pruning_stats: Default::default(),
        };

        Ok((
            statistics,
            Partitions::create(PartitionsShuffleKind::Seq, partitions),
        ))
    } else {
        let size = files.iter().map(|f| f.size as usize).sum();
        // assuming all fields are empty
        let max_rows = std::cmp::max(size / (stage_table_info.schema.fields.len() + 1), 1);
        let statistics = PartStatistics {
            snapshot: None,
            read_rows: max_rows,
            read_bytes: size,
            partitions_scanned: files.len(),
            partitions_total: files.len(),
            is_exact: false,
            pruning_stats: Default::default(),
        };

        let partitions = files
            .into_iter()
            .filter(|f| f.size > 0)
            .map(|v| {
                let part = SingleFilePartition {
                    path: v.path.clone(),
                    size: v.size as usize,
                };
                let part_info: Box<dyn PartInfo> = Box::new(part);
                Arc::new(part_info)
            })
            .collect::<Vec<_>>();

        Ok((
            statistics,
            Partitions::create(PartitionsShuffleKind::Seq, partitions),
        ))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct SplitRowBase {
    pub file: SingleFilePartition,
    pub seq_in_file: usize,
    pub num_file_splits: usize,
    pub offset: usize,
    pub size: usize,
}

#[typetag::serde(name = "split_row_base")]
impl PartInfo for SplitRowBase {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<SplitRowBase>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.file.path.hash(&mut s);
        self.offset.hash(&mut s);
        self.size.hash(&mut s);
        s.finish()
    }
}

#[allow(dead_code)]
async fn split_file(
    op: &Operator,
    file: &StageFileInfo,
    format: &CsvFileFormatParams,
    split_size: u64,
    probe_size: u64,
) -> Result<Option<Vec<SplitRowBase>>> {
    let n = file.size / split_size;
    if n <= 1 {
        return Ok(None);
    }

    let mut offsets = Vec::with_capacity(n as _);
    offsets.push(0_usize);
    for start in (1..n).map(|i| (split_size * i)) {
        let buf = op
            .read_with(&file.path)
            .range(start..start + probe_size)
            .await?
            .to_bytes();
        let mut temp = [0; 1024];
        let reader = CsvInputFormat::create_reader(format)?;
        let offset = match next_row_start(reader, &buf, &mut temp) {
            None => return Ok(None),
            Some(offset) => start as usize + offset,
        };
        offsets.push(offset);
    }

    let num_file_splits = n as _;
    Ok(Some(
        offsets
            .into_iter()
            .chain(Some(file.size as _))
            .enumerate()
            .map_windows(|&[(seq_in_file, offset), (_, end)]| SplitRowBase {
                file: SingleFilePartition {
                    path: file.path.clone(),
                    size: file.size as _,
                },
                seq_in_file,
                num_file_splits,
                offset,
                size: end - offset,
            })
            .collect(),
    ))
}

#[allow(dead_code)]
fn next_row_start(mut reader: csv_core::Reader, buf: &[u8], temp: &mut [u8]) -> Option<usize> {
    use csv_core::ReadFieldResult::*;
    let mut readded = 0;
    loop {
        let (result, n, _) = reader.read_field(&buf[readded..], temp);
        match result {
            Field { record_end } => {
                readded += n;
                if record_end {
                    return Some(readded);
                }
            }
            InputEmpty | OutputFull | End => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use databend_common_formats::RecordDelimiter;
    use databend_common_storage::StageFileStatus;
    use opendal::services;

    use super::*;

    #[tokio::test]
    async fn test_split_file_with_memory_operator() -> Result<()> {
        let operator = Operator::new(services::Memory::default())?.finish();
        let path = "test.csv".to_string();

        let mut data = Vec::new();
        for i in 0..327680 {
            let _ = writeln!(data, "{i},asdfe,\"eeess\",");
        }

        operator.write(&path, data.clone()).await?;

        let file = StageFileInfo {
            path: path.clone(),
            size: data.len() as u64,
            md5: None,
            last_modified: None,
            etag: None,
            status: StageFileStatus::NeedCopy,
            creator: None,
        };

        let format = CsvFileFormatParams::default();
        let split_size = 512 * 1024;
        let splits = split_file(&operator, &file, &format, split_size, 1024)
            .await?
            .expect("expected file splits");

        assert_eq!(splits.len(), splits[0].num_file_splits);

        let mut counter = 0;
        for (index, split) in splits.iter().enumerate() {
            assert_eq!(split.file.size, data.len());
            assert_eq!(split.seq_in_file, index);

            let start = split.offset as u64;
            let end = start + split.size as u64;
            let buf = operator.read_with(&path).range(start..end).await?;

            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(format.field_delimiter.as_bytes()[0])
                .quote(format.quote.as_bytes()[0])
                .escape((!format.escape.is_empty()).then(|| format.escape.as_bytes()[0]))
                .terminator(match format.record_delimiter.as_str().try_into()? {
                    RecordDelimiter::Crlf => csv::Terminator::CRLF,
                    RecordDelimiter::Any(v) => csv::Terminator::Any(v),
                })
                .has_headers(false)
                .from_reader(buf.clone());

            for result in rdr.records() {
                let record = result.unwrap();
                let i = record.get(0).unwrap().parse::<usize>().unwrap();
                assert_eq!(i, counter);
                counter += 1;
            }
        }

        Ok(())
    }
}
