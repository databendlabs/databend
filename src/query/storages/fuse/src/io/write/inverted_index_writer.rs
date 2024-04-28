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
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::ScalarRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use opendal::Operator;
use tantivy::indexer::UserOperation;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::JsonObjectOptions;
use tantivy::schema::OwnedValue;
use tantivy::schema::Schema;
use tantivy::schema::TantivyDocument;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::Language;
use tantivy::tokenizer::LowerCaser;
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
use tantivy_jieba::JiebaTokenizer;

use crate::index::build_tantivy_footer;
use crate::io::write_data;

pub struct InvertedIndexWriter {
    schema: DataSchema,
    index_writer: IndexWriter,
    operations: Vec<UserOperation>,
}

impl InvertedIndexWriter {
    pub fn try_create(
        schema: DataSchema,
        index_options: &BTreeMap<String, String>,
    ) -> Result<InvertedIndexWriter> {
        let (index_schema, _) = create_index_schema(&schema, index_options)?;

        let index_settings = IndexSettings {
            sort_by_field: None,
            ..Default::default()
        };

        let tokenizer_manager = create_tokenizer_manager(index_options);

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

        let mut types = Vec::with_capacity(self.schema.num_fields());
        for field in self.schema.fields() {
            let ty = field.data_type().remove_nullable();
            types.push(ty);
        }
        for i in 0..block.num_rows() {
            let mut doc = TantivyDocument::new();
            for (j, typ) in types.iter().enumerate() {
                let field = Field::from_field_id(j as u32);
                let column = block.get_by_offset(j);
                match unsafe { column.value.index_unchecked(i) } {
                    ScalarRef::String(text) => doc.add_text(field, text),
                    ScalarRef::Variant(jsonb_val) => {
                        // only support object JSON, other JSON type will not add index.
                        if let Ok(Some(obj_val)) = jsonb::to_serde_json_object(jsonb_val) {
                            let object: BTreeMap<String, OwnedValue> = obj_val
                                .into_iter()
                                .map(|(key, value)| (key, OwnedValue::from(value)))
                                .collect();
                            doc.add_object(field, object);
                        } else {
                            doc.add_object(field, BTreeMap::new());
                        }
                    }
                    _ => {
                        if typ == &DataType::Variant {
                            doc.add_object(field, BTreeMap::new());
                        } else {
                            doc.add_text(field, "");
                        }
                    }
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

        // the number of offsets, used for multi index segments in one file
        let nums = offsets.len() as u32;
        for offset in offsets {
            writer.write_all(&offset.to_le_bytes())?;
        }
        writer.write_all(&nums.to_le_bytes())?;

        writer.flush()?;

        Ok(())
    }

    fn build_footer<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<usize> {
        let buf = build_tantivy_footer(bytes)?;
        let len = writer.write(&buf)?;
        Ok(len)
    }
}

// Create tokenizer can handle both Chinese and English
pub(crate) fn create_tokenizer_manager(
    index_options: &BTreeMap<String, String>,
) -> TokenizerManager {
    let tokenizer_manager = TokenizerManager::new();

    let filters: HashSet<String> = match index_options.get("filters") {
        Some(filters_str) => filters_str.split(',').map(|v| v.to_string()).collect(),
        None => HashSet::new(),
    };

    // add lower case filter by default, so that the search can match
    // all the rows regardless of whether it is uppercase or lowercase
    let (english_analyzer, chinese_analyzer) = if filters.is_empty() {
        let english_analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build();
        let chinese_analyzer = TextAnalyzer::builder(JiebaTokenizer {})
            .filter(LowerCaser)
            .build();

        (english_analyzer, chinese_analyzer)
    } else {
        let mut english_analyzer =
            TextAnalyzer::builder(SimpleTokenizer::default()).filter_dynamic(LowerCaser);
        let mut chinese_analyzer =
            TextAnalyzer::builder(JiebaTokenizer {}).filter_dynamic(LowerCaser);

        // add optional filters
        // remove English stop words, like "a", "an", "and", etc.
        if filters.contains("english_stop") {
            english_analyzer =
                english_analyzer.filter_dynamic(StopWordFilter::new(Language::English).unwrap());
            chinese_analyzer =
                chinese_analyzer.filter_dynamic(StopWordFilter::new(Language::English).unwrap());
        }
        // English stemmer maps different forms of the same word to a common word.
        // for example, "walking" and "walked" will be mapped to "walk".
        if filters.contains("english_stemmer") {
            english_analyzer = english_analyzer.filter_dynamic(Stemmer::new(Language::English));
            chinese_analyzer = chinese_analyzer.filter_dynamic(Stemmer::new(Language::English));
        }
        // remove Chinese stop words, which currently only supports Chinese punctuation is supported.
        if filters.contains("chinese_stop") {
            // Punctuation tokens to remove copied from lucene
            // https://github.com/apache/lucene/blob/main/lucene/analysis/smartcn/src/resources/org/apache/lucene/analysis/cn/smart/stopwords.txt
            chinese_analyzer = chinese_analyzer.filter_dynamic(StopWordFilter::remove(vec![
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
            ]));
        }
        (english_analyzer.build(), chinese_analyzer.build())
    };

    tokenizer_manager.register("english", english_analyzer);
    tokenizer_manager.register("chinese", chinese_analyzer);
    tokenizer_manager
}

pub(crate) fn create_index_schema(
    schema: &DataSchema,
    index_options: &BTreeMap<String, String>,
) -> Result<(Schema, Vec<Field>)> {
    let tokenizer_name = index_options
        .get("tokenizer")
        .cloned()
        .unwrap_or("english".to_string());

    // There are three types of index records that support different needs.
    //
    // 1. `basic`: only stores `DocId`, takes up minimal space,
    //    but can't search for phrase terms, like `"quick brown fox"`.
    // 2. `freq`: store `DocId` and term frequency, takes up medium space,
    //    and also can't search for phrase terms, but can give better scoring.
    // 3. `position`: store `DocId`, term frequency, and positions,
    //    take up most space, have better scoring, and can search for phrase terms.
    let index_record: IndexRecordOption = match index_options.get("index_record") {
        Some(v) => serde_json::from_str(v)?,
        None => IndexRecordOption::WithFreqsAndPositions,
    };

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer(&tokenizer_name)
        .set_index_option(index_record);
    let text_options = TextOptions::default().set_indexing_options(text_field_indexing.clone());
    let json_options = JsonObjectOptions::default().set_indexing_options(text_field_indexing);

    let mut schema_builder = Schema::builder();
    let mut index_fields = Vec::with_capacity(schema.fields.len());
    for field in schema.fields() {
        let index_field = match field.data_type().remove_nullable() {
            DataType::String => schema_builder.add_text_field(field.name(), text_options.clone()),
            DataType::Variant => schema_builder.add_json_field(field.name(), json_options.clone()),
            _ => {
                return Err(ErrorCode::IllegalDataType(format!(
                    "inverted index only support String and Variant type, but got {}",
                    field.data_type()
                )));
            }
        };
        index_fields.push(index_field);
    }
    let index_schema = schema_builder.build();

    Ok((index_schema, index_fields))
}
