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

// Copyright (c) 2018 by the tantivy project authors
// (https://github.com/quickwit-oss/tantivy), as listed in the AUTHORS file.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

use std::io::Write;
use std::path::Path;

use crc32fast::Hasher;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::ScalarRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;
use tantivy::schema::Document;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::Language;
use tantivy::tokenizer::LowerCaser;
use tantivy::tokenizer::RemoveLongFilter;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::tokenizer::Stemmer;
use tantivy::tokenizer::StopWordFilter;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Directory;
use tantivy::Index;
use tantivy::IndexBuilder;
use tantivy::IndexSettings;
use tantivy::IndexWriter;
use tantivy::SegmentComponent;
use tantivy::UserOperation;
use tantivy_common::BinarySerializable;
use tantivy_jieba::JiebaTokenizer;

use crate::io::write_data;

// tantivy version is used to generate the footer data

// Index major version.
const INDEX_MAJOR_VERSION: u32 = 0;
// Index minor version.
const INDEX_MINOR_VERSION: u32 = 21;
// Index patch version.
const INDEX_PATCH_VERSION: u32 = 1;
// Index format version.
const INDEX_FORMAT_VERSION: u32 = 5;

// The magic byte of the footer to identify corruption
// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// Structure version for the index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
    index_format_version: u32,
}

/// A Footer is appended every part of data, like tantivy file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Footer {
    pub version: Version,
    pub crc: CrcHashU32,
}

impl Footer {
    pub fn new(crc: CrcHashU32) -> Self {
        let version = Version {
            major: INDEX_MAJOR_VERSION,
            minor: INDEX_MINOR_VERSION,
            patch: INDEX_PATCH_VERSION,
            index_format_version: INDEX_FORMAT_VERSION,
        };
        Footer { version, crc }
    }

    pub fn append_footer<W: std::io::Write>(&self, write: &mut W) -> Result<()> {
        let footer_payload_len = write.write(serde_json::to_string(&self)?.as_ref())?;
        BinarySerializable::serialize(&(footer_payload_len as u32), write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }
}

pub struct InvertedIndexWriter {
    schema: DataSchema,
    index_writer: IndexWriter,
    operations: Vec<UserOperation>,
}

impl InvertedIndexWriter {
    pub fn try_create(schema: DataSchema, tokenizer_name: &str) -> Result<InvertedIndexWriter> {
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer(tokenizer_name)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing);

        let mut schema_builder = Schema::builder();
        let mut index_fields = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            if field.data_type().remove_nullable() != DataType::String {
                return Err(ErrorCode::IllegalDataType(format!(
                    "inverted index only support String type, but got {}",
                    field.data_type()
                )));
            }
            let index_field = schema_builder.add_text_field(field.name(), text_options.clone());
            index_fields.push(index_field);
        }
        let index_schema = schema_builder.build();

        let index_settings = IndexSettings {
            sort_by_field: None,
            ..Default::default()
        };

        let tokenizer_manager = create_tokenizer_manager();

        let index_builder = IndexBuilder::new()
            .settings(index_settings)
            .schema(index_schema.clone())
            .tokenizers(tokenizer_manager.clone());

        let index = index_builder.create_in_ram()?;
        let index_writer = index.writer(DEFAULT_BLOCK_BUFFER_SIZE)?;
        let operations = Vec::new();

        Ok(Self {
            schema,
            index_writer,
            operations,
        })
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if block.num_columns() != self.schema.num_fields() {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched. Data columns length: {}, schema fields length: {}",
                block.num_columns(),
                self.schema.num_fields()
            )));
        }
        for (column, field) in block.columns().iter().zip(self.schema.fields().iter()) {
            if &column.data_type != field.data_type() {
                return Err(ErrorCode::TableSchemaMismatch(format!(
                    "Data schema mismatched (col name: {}). Data column type is {:?}, but schema field type is {:?}",
                    field.name(),
                    column.data_type,
                    field.data_type()
                )));
            }
        }

        for i in 0..block.num_rows() {
            let mut doc = Document::new();
            for j in 0..block.num_columns() {
                let field = Field::from_field_id(j as u32);
                let column = block.get_by_offset(j);
                if let ScalarRef::String(text) = unsafe { column.value.index_unchecked(i) } {
                    doc.add_text(field, text);
                } else {
                    doc.add_text(field, "");
                }
            }
            self.operations.push(UserOperation::Add(doc));
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn finalize(mut self, operator: &Operator, index_location: String) -> Result<()> {
        let _ = self.index_writer.run(self.operations);
        let _ = self.index_writer.commit()?;
        let index = self.index_writer.index();

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        Self::write_index(&mut buffer, index).await?;
        write_data(buffer, operator, &index_location).await?;

        Ok(())
    }

    // The tantivy index data consists of eight files.
    // `.managed.json` file stores the name of the file that the index contains, for example:
    // [
    //   "94bce521d5bc4eccbf3e7a0212093622.pos",
    //   "94bce521d5bc4eccbf3e7a0212093622.fieldnorm",
    //   "94bce521d5bc4eccbf3e7a0212093622.fast",
    //   "meta.json",
    //   "94bce521d5bc4eccbf3e7a0212093622.term",
    //   "94bce521d5bc4eccbf3e7a0212093622.store",
    //   "94bce521d5bc4eccbf3e7a0212093622.idx"
    // ]
    //
    // `meta.json` file store the meta information associated with the index, for example:
    // {
    //   "index_settings": {
    //     "docstore_compression": "lz4",
    //     "docstore_blocksize": 16384
    //   },
    //   "segments":[{
    //     "segment_id": "94bce521-d5bc-4ecc-bf3e-7a0212093622",
    //     "max_doc": 6,
    //     "deletes": null
    //   }],
    //   "schema":[{
    //     "name": "title",
    //     "type": "text",
    //     "options": {
    //       "indexing": {
    //         "record": "position",
    //         "fieldnorms": true,
    //         "tokenizer": "en"
    //       },
    //       "stored": false,
    //       "fast": false
    //     }
    //   }, {
    //     "name": "content",
    //     "type": "text",
    //     "options": {
    //       "indexing": {
    //         "record": "position",
    //         "fieldnorms": true,
    //         "tokenizer": "en"
    //       },
    //       "stored": false,
    //       "fast": false
    //     }
    //   }],
    //   "opstamp":0
    // }
    //
    // `terms` file stores the term dictionary, the value is
    // an address into the `postings` file and the `positions` file.
    // `postings` file stores the lists of document ids and freqs.
    // `positions` file stores the positions of terms in each document.
    // `field norms` file stores the sum of the length of the term.
    // `fast fields` file stores column-oriented documents.
    // `store` file stores row-oriented documents.
    //
    // More details can be seen here
    // https://github.com/quickwit-oss/tantivy/blob/main/src/index/segment_component.rs#L8
    //
    // We merge the data from these files into one file and
    // record the offset to read each part of the data.
    #[async_backtrace::framed]
    async fn write_index<W: Write>(mut writer: &mut W, index: &Index) -> Result<()> {
        let directory = index.directory();

        let managed_filepath = Path::new(".managed.json");
        let managed_bytes = directory.atomic_read(managed_filepath)?;

        let meta_filepath = Path::new("meta.json");
        let meta_data = directory.atomic_read(meta_filepath)?;

        let meta_string = std::str::from_utf8(&meta_data)?;
        let meta_val: serde_json::Value = serde_json::from_str(meta_string)?;
        let meta_json: String = serde_json::to_string(&meta_val)?;

        let segments = index.searchable_segments()?;
        let segment = &segments[0];

        let fast_fields = segment.open_read(SegmentComponent::FastFields)?;
        let fast_fields_bytes = fast_fields.read_bytes()?;

        let store = segment.open_read(SegmentComponent::Store)?;
        let store_bytes = store.read_bytes()?;

        let field_norms = segment.open_read(SegmentComponent::FieldNorms)?;
        let field_norms_bytes = field_norms.read_bytes()?;

        let positions = segment.open_read(SegmentComponent::Positions)?;
        let positions_bytes = positions.read_bytes()?;

        let postings = segment.open_read(SegmentComponent::Postings)?;
        let postings_bytes = postings.read_bytes()?;

        let terms = segment.open_read(SegmentComponent::Terms)?;
        let terms_bytes = terms.read_bytes()?;

        // write each tantivy files as part of data
        let mut fast_fields_length = writer.write(&fast_fields_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &fast_fields_bytes)?;
        fast_fields_length += footer_length;

        let mut store_length = writer.write(&store_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &store_bytes)?;
        store_length += footer_length;

        let mut field_norms_length = writer.write(&field_norms_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &field_norms_bytes)?;
        field_norms_length += footer_length;

        let mut positions_length = writer.write(&positions_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &positions_bytes)?;
        positions_length += footer_length;

        let mut postings_length = writer.write(&postings_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &postings_bytes)?;
        postings_length += footer_length;

        let mut terms_length = writer.write(&terms_bytes)?;
        let footer_length = Self::build_footer(&mut writer, &terms_bytes)?;
        terms_length += footer_length;

        let meta_length = writer.write(meta_json.as_bytes())?;
        let managed_length = writer.write(&managed_bytes)?;

        // write offsets of each parts
        let mut offset: u32 = 0;
        let mut offsets = Vec::with_capacity(8);
        offset += fast_fields_length as u32;
        offsets.push(offset);

        offset += store_length as u32;
        offsets.push(offset);

        offset += field_norms_length as u32;
        offsets.push(offset);

        offset += positions_length as u32;
        offsets.push(offset);

        offset += postings_length as u32;
        offsets.push(offset);

        offset += terms_length as u32;
        offsets.push(offset);

        offset += meta_length as u32;
        offsets.push(offset);

        offset += managed_length as u32;
        offsets.push(offset);

        for offset in offsets {
            writer.write_all(&offset.to_le_bytes())?;
        }

        writer.flush()?;

        Ok(())
    }

    fn build_footer<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<usize> {
        let mut hasher = Hasher::new();
        hasher.update(bytes);
        let crc = hasher.finalize();

        let footer = Footer::new(crc);
        let mut buf = Vec::new();
        footer.append_footer(&mut buf)?;

        let len = writer.write(&buf)?;
        Ok(len)
    }
}

// Create tokenizer can handle both Chinese and English
pub(crate) fn create_tokenizer_manager() -> TokenizerManager {
    let tokenizer_manager = TokenizerManager::new();
    tokenizer_manager.register(
        "english",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(StopWordFilter::new(Language::English).unwrap())
            .filter(Stemmer::new(Language::English))
            .build(),
    );
    tokenizer_manager.register(
        "chinese",
        TextAnalyzer::builder(JiebaTokenizer {})
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            // Punctuation tokens to remove copied from lucene
            // https://github.com/apache/lucene/blob/main/lucene/analysis/smartcn/src/resources/org/apache/lucene/analysis/cn/smart/stopwords.txt
            .filter(StopWordFilter::remove(vec![
                ",".to_string(),
                ".".to_string(),
                "`".to_string(),
                "-".to_string(),
                "_".to_string(),
                "=".to_string(),
                "?".to_string(),
                "'".to_string(),
                "|".to_string(),
                "\"".to_string(),
                "(".to_string(),
                ")".to_string(),
                "{".to_string(),
                "}".to_string(),
                "[".to_string(),
                "]".to_string(),
                "<".to_string(),
                ">".to_string(),
                "*".to_string(),
                "#".to_string(),
                "&".to_string(),
                "^".to_string(),
                "$".to_string(),
                "@".to_string(),
                "!".to_string(),
                "~".to_string(),
                ":".to_string(),
                ";".to_string(),
                "+".to_string(),
                "/".to_string(),
                "\\".to_string(),
                "《".to_string(),
                "》".to_string(),
                "—".to_string(),
                "－".to_string(),
                "，".to_string(),
                "。".to_string(),
                "、".to_string(),
                "：".to_string(),
                "；".to_string(),
                "！".to_string(),
                "·".to_string(),
                "？".to_string(),
                "“".to_string(),
                "”".to_string(),
                "）".to_string(),
                "（".to_string(),
                "【".to_string(),
                "】".to_string(),
                "［".to_string(),
                "］".to_string(),
                "●".to_string(),
                "　".to_string(),
            ]))
            .filter(StopWordFilter::new(Language::English).unwrap())
            .filter(Stemmer::new(Language::English))
            .build(),
    );
    tokenizer_manager
}
