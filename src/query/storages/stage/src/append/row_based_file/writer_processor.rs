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
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::CompressCodec;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use encoding_rs::EncoderResult;
use encoding_rs::Encoding;
use encoding_rs::UTF_8;
use opendal::Operator;

use super::buffers::FileOutputBuffers;
use crate::append::UnloadOutput;
use crate::append::output::DataSummary;
use crate::append::path::unload_path;

pub struct RowBasedFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    info: CopyIntoLocationInfo,

    // always blocks for a whole file if not empty
    input_data: Option<DataBlock>,
    // always the data for a whole file if not empty
    file_to_write: Option<(Vec<u8>, DataSummary, Option<Arc<str>>)>,

    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    data_accessor: Operator,
    prefix: Vec<u8>,

    query_id: String,
    group_id: usize,
    batch_id: usize,

    compression: Option<CompressAlgorithm>,
    suffix: String,
}

impl RowBasedFileWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        data_accessor: Operator,
        prefix: Vec<u8>,
        query_id: String,
        group_id: usize,
        compression: Option<CompressAlgorithm>,
        suffix: &str,
    ) -> Result<ProcessorPtr> {
        let unload_output = UnloadOutput::create(info.options.detailed_output);
        Ok(ProcessorPtr::create(Box::new(RowBasedFileWriter {
            info,
            input,
            input_data: None,
            data_accessor,
            prefix,
            query_id,
            group_id,
            batch_id: 0,
            file_to_write: None,
            compression,
            suffix: suffix.to_string(),
            output,
            unload_output,
            unload_output_blocks: None,
        })))
    }
}

fn resolve_output_encoding(format: &FileFormatParams) -> Result<Option<(&str, &'static Encoding)>> {
    let label = match format {
        FileFormatParams::Csv(params) => params.encoding.as_str(),
        FileFormatParams::Text(params) => params.encoding.as_str(),
        _ => return Ok(None),
    };

    let encoding = Encoding::for_label_no_replacement(label.trim().as_bytes())
        .ok_or_else(|| ErrorCode::BadArguments(format!("unsupported file encoding '{}'", label)))?;
    if encoding == UTF_8 {
        Ok(None)
    } else {
        Ok(Some((label, encoding)))
    }
}

fn transcode_output(data: Vec<u8>, format: &FileFormatParams) -> Result<Vec<u8>> {
    let Some((label, encoding)) = resolve_output_encoding(format)? else {
        return Ok(data);
    };

    let mut encoder = encoding.new_encoder();
    let capacity = encoder
        .max_buffer_length_from_utf8_if_no_unmappables(data.len())
        .ok_or_else(|| {
            ErrorCode::BadBytes(format!(
                "failed to encode unload data with encoding '{}': output too large",
                label
            ))
        })?;
    let src = std::str::from_utf8(&data).map_err(|err| {
        ErrorCode::BadBytes(format!(
            "failed to encode unload data with encoding '{}': invalid internal utf-8: {}",
            label, err
        ))
    })?;
    let mut output = vec![0; capacity];
    let (result, read, written) =
        encoder.encode_from_utf8_without_replacement(src, &mut output, true);
    match result {
        EncoderResult::InputEmpty => {
            debug_assert_eq!(read, src.len());
            output.truncate(written);
            Ok(output)
        }
        EncoderResult::OutputFull => {
            unreachable!("reserved enough output buffer for unload transcoding")
        }
        EncoderResult::Unmappable(ch) => Err(ErrorCode::BadBytes(format!(
            "failed to encode unload data with encoding '{}': character '{}' is not representable",
            label, ch
        ))),
    }
}

#[async_trait]
impl Processor for RowBasedFileWriter {
    fn name(&self) -> String {
        "RowBasedFileWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if self.file_to_write.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if self.input_data.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if self.unload_output.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            if self.unload_output_blocks.is_none() {
                self.unload_output_blocks = Some(self.unload_output.to_block_partial().into());
            }
            if self.output.can_push() {
                if let Some(block) = self.unload_output_blocks.as_mut().unwrap().pop_front() {
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                } else {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            } else {
                Ok(Event::NeedConsume)
            }
        } else if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        let block = self.input_data.take().unwrap();
        let block_meta = block.get_owned_meta().unwrap();
        let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
        let partition = buffers.partition.clone();
        let size = buffers
            .buffers
            .iter()
            .map(|b| b.buffer.len())
            .sum::<usize>();
        let row_counts = buffers.buffers.iter().map(|b| b.row_counts).sum::<usize>();
        let mut output = Vec::with_capacity(self.prefix.len() + size);
        output.extend_from_slice(self.prefix.as_slice());
        for b in buffers.buffers {
            output.extend_from_slice(b.buffer.as_slice());
        }
        output = transcode_output(output, &self.info.stage.file_format_params)?;
        let input_bytes = output.len();
        if let Some(compression) = self.compression {
            output = if compression == CompressAlgorithm::Zip {
                let name = format!("unload_{}{}", self.batch_id, self.suffix);
                CompressCodec::compress_all_zip(&output, name.as_str())?
            } else {
                CompressCodec::from(compression).compress_all(&output)?
            };
        }
        let output_bytes = output.len();
        let summary = DataSummary {
            row_counts,
            input_bytes,
            output_bytes,
        };
        self.file_to_write = Some((output, summary, partition));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let (data, summary, partition) = mem::take(&mut self.file_to_write).unwrap();
        let path = unload_path(
            &self.info,
            &self.query_id,
            self.group_id,
            self.batch_id,
            self.compression,
            partition.as_deref(),
        );
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_app::principal::CsvFileFormatParams;
    use databend_common_meta_app::principal::TextFileFormatParams;
    use encoding_rs::GBK;

    use super::*;

    #[test]
    fn test_transcode_output_csv_gbk() {
        let format = FileFormatParams::Csv(CsvFileFormatParams {
            encoding: "gbk".to_string(),
            ..CsvFileFormatParams::default()
        });

        let got = transcode_output("张三,1\n".as_bytes().to_vec(), &format).unwrap();
        let (want, _, had_errors) = GBK.encode("张三,1\n");
        assert!(!had_errors);
        assert_eq!(got, want.as_ref());
    }

    #[test]
    fn test_transcode_output_text_utf8_passthrough() {
        let format = FileFormatParams::Text(TextFileFormatParams::default());
        let input = "hello\t1\n".as_bytes().to_vec();

        let got = transcode_output(input.clone(), &format).unwrap();
        assert_eq!(got, input);
    }

    #[test]
    fn test_transcode_output_unmappable_char() {
        let format = FileFormatParams::Csv(CsvFileFormatParams {
            encoding: "gbk".to_string(),
            ..CsvFileFormatParams::default()
        });

        let err = transcode_output("😀,1\n".as_bytes().to_vec(), &format).unwrap_err();
        assert!(err.message().contains("not representable"));
    }
}
