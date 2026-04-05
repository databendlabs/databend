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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use encoding_rs::CoderResult;
use encoding_rs::Decoder;
use encoding_rs::DecoderResult;
use encoding_rs::Encoding;
use encoding_rs::UTF_8;

use crate::read::row_based::batch::BytesBatch;

pub struct DecodingTransformer {
    label: String,
    error_mode: EncodingErrorMode,
    encoding: &'static Encoding,
    decoder: Option<Decoder>,
    path: Option<String>,
}

impl DecodingTransformer {
    pub fn try_create(label: String, encoding_error: String) -> Result<Self> {
        let encoding = resolve_encoding(&label)?;
        let error_mode = EncodingErrorMode::parse(&encoding_error)?;
        Ok(Self {
            label,
            error_mode,
            encoding,
            decoder: None,
            path: None,
        })
    }

    pub fn needs_processing(label: &str, encoding_error: &str) -> Result<bool> {
        Ok(resolve_encoding(label)? != UTF_8
            || EncodingErrorMode::parse(encoding_error)? != EncodingErrorMode::Strict)
    }

    fn new_file(&mut self, path: String) {
        self.path = Some(path);
        self.decoder = Some(self.encoding.new_decoder_with_bom_removal());
    }

    fn file_decode_error(&self, path: &str) -> ErrorCode {
        ErrorCode::BadBytes(format!(
            "failed to decode file {path} with encoding '{}' and encoding_error='{}'",
            self.label, self.error_mode
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EncodingErrorMode {
    Strict,
    Replace,
}

impl EncodingErrorMode {
    fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "strict" => Ok(Self::Strict),
            "replace" => Ok(Self::Replace),
            _ => Err(ErrorCode::BadArguments(format!(
                "unsupported encoding_error '{}'",
                value
            ))),
        }
    }
}

impl std::fmt::Display for EncodingErrorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict => write!(f, "strict"),
            Self::Replace => write!(f, "replace"),
        }
    }
}

fn resolve_encoding(label: &str) -> Result<&'static Encoding> {
    Encoding::for_label_no_replacement(label.trim().as_bytes())
        .ok_or_else(|| ErrorCode::BadArguments(format!("unsupported file encoding '{}'", label)))
}

impl AccumulatingTransform for DecodingTransformer {
    const NAME: &'static str = "EncodingTransformer";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();

        match &self.path {
            None => self.new_file(batch.path.clone()),
            Some(path) if path != &batch.path => self.new_file(batch.path.clone()),
            Some(_) => {}
        }

        let decoder = self
            .decoder
            .as_mut()
            .expect("encoding decoder must be initialized");
        let mut output = match self.error_mode {
            EncodingErrorMode::Strict => {
                let capacity =
                    match decoder.max_utf8_buffer_length_without_replacement(batch.data.len()) {
                        Some(capacity) => capacity,
                        None => return Err(self.file_decode_error(&batch.path)),
                    };
                String::with_capacity(capacity)
            }
            EncodingErrorMode::Replace => {
                let capacity = match decoder.max_utf8_buffer_length(batch.data.len()) {
                    Some(capacity) => capacity,
                    None => return Err(self.file_decode_error(&batch.path)),
                };
                String::with_capacity(capacity)
            }
        };

        match self.error_mode {
            EncodingErrorMode::Strict => {
                let (result, read) = decoder.decode_to_string_without_replacement(
                    &batch.data,
                    &mut output,
                    batch.is_eof,
                );
                if !matches!(result, DecoderResult::InputEmpty) {
                    return Err(self.file_decode_error(&batch.path));
                }
                debug_assert_eq!(read, batch.data.len());
            }
            EncodingErrorMode::Replace => {
                let (result, read, _) =
                    decoder.decode_to_string(&batch.data, &mut output, batch.is_eof);
                if !matches!(result, CoderResult::InputEmpty) {
                    unreachable!("reserved enough output buffer");
                }
                debug_assert_eq!(read, batch.data.len());
            }
        }

        if batch.is_eof {
            self.decoder = None;
            self.path = None;
        }

        if output.is_empty() && !batch.is_eof {
            return Ok(vec![]);
        }

        Ok(vec![DataBlock::empty_with_meta(Box::new(BytesBatch {
            data: output.into_bytes(),
            path: batch.path,
            offset: batch.offset,
            is_eof: batch.is_eof,
        }))])
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockMetaInfoDowncast;
    use encoding_rs::GBK;

    use super::*;

    #[test]
    fn test_encoding_transformer_needs_processing() {
        assert!(!DecodingTransformer::needs_processing("utf-8", "strict").unwrap());
        assert!(DecodingTransformer::needs_processing("gbk", "strict").unwrap());
        assert!(DecodingTransformer::needs_processing("utf-8", "replace").unwrap());
    }

    #[test]
    fn test_encoding_transformer_streaming_decode() {
        let (encoded, _, had_errors) = GBK.encode("张三,1\n");
        assert!(!had_errors);

        let mut transformer =
            DecodingTransformer::try_create("gbk".to_string(), "strict".to_string()).unwrap();
        let first = transformer
            .transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
                data: encoded[..1].to_vec(),
                path: "test.csv".to_string(),
                offset: 0,
                is_eof: false,
            })))
            .unwrap();
        assert!(first.is_empty());

        let second = transformer
            .transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
                data: encoded[1..].to_vec(),
                path: "test.csv".to_string(),
                offset: 1,
                is_eof: true,
            })))
            .unwrap();
        assert_eq!(second.len(), 1);

        let batch = second
            .into_iter()
            .next()
            .unwrap()
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();
        assert_eq!(String::from_utf8(batch.data).unwrap(), "张三,1\n");
    }

    #[test]
    fn test_encoding_transformer_strict_invalid_utf8() {
        let mut transformer =
            DecodingTransformer::try_create("utf-8".to_string(), "strict".to_string()).unwrap();
        let err = transformer
            .transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
                data: b"ab\xffcd\n".to_vec(),
                path: "bad.csv".to_string(),
                offset: 0,
                is_eof: true,
            })))
            .unwrap_err();

        assert!(err.message().contains("encoding_error='strict'"));
    }

    #[test]
    fn test_encoding_transformer_replace_invalid_utf8() {
        let mut transformer =
            DecodingTransformer::try_create("utf-8".to_string(), "replace".to_string()).unwrap();
        let blocks = transformer
            .transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
                data: b"ab\xffcd\n".to_vec(),
                path: "bad.csv".to_string(),
                offset: 0,
                is_eof: true,
            })))
            .unwrap();

        let batch = blocks
            .into_iter()
            .next()
            .unwrap()
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();
        assert_eq!(String::from_utf8(batch.data).unwrap(), "ab\u{fffd}cd\n");
    }
}
