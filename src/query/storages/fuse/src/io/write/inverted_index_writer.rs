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
use std::sync::Arc;

use arrow_ipc::writer::write_message;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_storages_common_index::extract_component_fields;
use databend_storages_common_index::extract_fsts;
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
use tantivy::IndexBuilder;
use tantivy::IndexSettings;
use tantivy::IndexWriter;
use tantivy::SegmentComponent;
use tantivy_jieba::JiebaTokenizer;

pub struct InvertedIndexWriter {
    schema: DataSchemaRef,
    index_writer: IndexWriter,
    operations: Vec<UserOperation>,
}

impl InvertedIndexWriter {
    pub fn try_create(
        schema: DataSchemaRef,
        index_options: &BTreeMap<String, String>,
    ) -> Result<InvertedIndexWriter> {
        let (index_schema, _) = create_index_schema(schema.clone(), index_options)?;

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

    pub fn add_block(&mut self, source_schema: &TableSchemaRef, block: &DataBlock) -> Result<()> {
        let mut field_indexes = Vec::with_capacity(self.schema.num_fields());
        for field in self.schema.fields() {
            let ty = field.data_type().remove_nullable();
            let field_index = source_schema.index_of(field.name().as_str())?;
            field_indexes.push((field_index, ty))
        }

        for i in 0..block.num_rows() {
            let mut doc = TantivyDocument::new();
            for (j, (field_index, ty)) in field_indexes.iter().enumerate() {
                let field = Field::from_field_id(j as u32);
                let column = block.get_by_offset(*field_index);
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
                        if ty == &DataType::Variant {
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
    pub fn finalize(mut self) -> Result<(TableSchema, DataBlock)> {
        let _ = self.index_writer.run(self.operations);
        let _ = self.index_writer.commit()?;
        let index = self.index_writer.index();

        let mut fields = Vec::new();
        let mut values = Vec::new();

        let segments = index.searchable_segments()?;
        let segment = &segments[0];

        let termdict_file = segment.open_read(SegmentComponent::Terms)?;
        extract_fsts(termdict_file, &mut fields, &mut values)?;

        let field_norms_file = segment.open_read(SegmentComponent::FieldNorms)?;
        extract_component_fields("fieldnorm", field_norms_file, &mut fields, &mut values)?;

        let posting_file = segment.open_read(SegmentComponent::Postings)?;
        extract_component_fields("idx", posting_file, &mut fields, &mut values)?;

        let position_file = segment.open_read(SegmentComponent::Positions)?;
        extract_component_fields("pos", position_file, &mut fields, &mut values)?;

        let inverted_index_schema = TableSchema::new(fields);

        let mut index_columns = Vec::with_capacity(values.len());
        for value in values.into_iter() {
            let index_value = Value::Scalar(value);
            index_columns.push(BlockEntry::new(DataType::Binary, index_value));
        }
        let inverted_index_block = DataBlock::new(index_columns, 1);

        Ok((inverted_index_schema, inverted_index_block))
    }
}

// inverted index block include 5 types of data,
// and each of which may have multiple fields.
// 1. `fst` used to check whether a term exist.
//    for example: fst-0, fst-1, ..
// 2. `term dict` records the idx and pos locations of each terms.
//    for example: term-0, term-1, ..
// 3. `idx` records the doc ids of each terms.
//    for example: idx-0, idx-1, ..
// 4. `pos` records the positions of each terms in doc.
//    for example: pos-0, pos-1, ..
// 5. `fieldnorms` records the number of tokens in each doc.
//    for example: fieldnorms-0, fieldnorms-1, ..
//
// write the value of columns first,
// and then the offsets of columns,
// finally the number of columns.
pub(crate) fn block_to_inverted_index(
    table_schema: &TableSchema,
    block: DataBlock,
    write_buffer: &mut Vec<u8>,
) -> Result<()> {
    let mut offsets = Vec::with_capacity(block.num_columns());
    for column in block.columns() {
        let value: Value<BinaryType> = column.value.try_downcast().unwrap();
        write_buffer.extend_from_slice(value.as_scalar().unwrap());
        let offset = write_buffer.len() as u32;
        offsets.push(offset);
    }

    // footer: schema + offsets + schema_len + meta_len
    let arrow_schema = Arc::new(ArrowSchema::from(table_schema));
    let generator = IpcDataGenerator {};
    let write_options = IpcWriteOptions::default();
    #[allow(deprecated)]
    let encoded = generator.schema_to_bytes(&arrow_schema, &write_options);
    let mut schema_buf = Vec::new();
    let (schema_len, _) = write_message(&mut schema_buf, encoded, &write_options)?;
    write_buffer.extend_from_slice(&schema_buf);

    let schema_len = schema_len as u32;
    let offset_len = (offsets.len() * 4) as u32;
    for offset in offsets {
        write_buffer.extend_from_slice(&offset.to_le_bytes());
    }
    let meta_len = schema_len + offset_len + 8;

    write_buffer.extend_from_slice(&schema_len.to_le_bytes());
    write_buffer.extend_from_slice(&meta_len.to_le_bytes());

    Ok(())
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
    schema: DataSchemaRef,
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
