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
use std::path::Path;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_storages_common_blocks::block_to_parquet_with_writer;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;
use jsonb::RawJsonb;
use jsonb::from_raw_jsonb;
use log::debug;
use opendal::Buffer;
use tantivy::Directory;
use tantivy::IndexBuilder;
use tantivy::IndexSettings;
use tantivy::IndexWriter;
use tantivy::index::SegmentComponent;
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
use tantivy_jieba::JiebaTokenizer;

use crate::index::build_tantivy_footer;
use crate::io::TableMetaLocationGenerator;
use crate::io::write::block_index::BlockIndexLowLevelColumnWriter;
use crate::io::write::block_index::BlockIndexLowLevelWriteContext;
use crate::io::write::block_index::BlockIndexLowLevelWriter;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::PendingIndexFile;
use crate::io::write::block_index::PendingInvertedIndex;
use crate::io::write::block_index::WrittenBlockIndexOutput;
use crate::io::write::block_index::WrittenIndexFile;
use crate::io::write::block_index::WrittenInvertedIndex;

#[derive(Clone)]
pub struct InvertedIndexBuilder {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) schema: DataSchema,
    pub(crate) options: BTreeMap<String, String>,
}

impl InvertedIndexBuilder {
    pub fn gen_inverted_index_location(&self, block_location: &Location) -> String {
        TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
            &block_location.0,
            &self.name,
            &self.version,
        )
    }
}

impl BlockIndexSpec for InvertedIndexBuilder {
    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>> {
        let location = self.gen_inverted_index_location(&context.block_location);
        Ok(Box::new(InvertedIndexBlockWriter {
            index_name: self.name.clone(),
            location: (location, 0),
            source_schema: context.physical_schema,
            writer: InvertedIndexWriter::try_create(Arc::new(self.schema.clone()), &self.options)?,
        }))
    }

    fn new_low_level_writer(
        &self,
        context: BlockIndexLowLevelWriteContext,
    ) -> Result<Box<dyn BlockIndexLowLevelWriter>> {
        let location = (self.gen_inverted_index_location(&context.block_location), 0);
        let write = context.create_write(&location);
        let field_indexes = self
            .schema
            .fields()
            .iter()
            .map(|field| context.physical_schema.index_of(field.name()))
            .collect::<Result<Vec<_>>>()?;
        let num_fields = context.physical_schema.num_fields();
        Ok(Box::new(InvertedIndexLowLevelWriter {
            index_name: self.name.clone(),
            location,
            field_indexes,
            columns: vec![None; num_fields],
            writer: Some(InvertedIndexWriter::try_create(
                Arc::new(self.schema.clone()),
                &self.options,
            )?),
            write: Some(write),
            next_field: 0,
            num_fields,
        }))
    }
}

struct InvertedIndexBlockWriter {
    index_name: String,
    location: Location,
    source_schema: TableSchemaRef,
    writer: InvertedIndexWriter,
}

impl BlockIndexWriter for InvertedIndexBlockWriter {
    fn write(&mut self, block: &DataBlock) -> Result<()> {
        self.writer.add_block(&self.source_schema, block)
    }

    fn finish(self: Box<Self>) -> Result<PendingBlockIndexOutput> {
        let data = self.writer.finalize()?;
        Ok(PendingBlockIndexOutput {
            inverted: vec![PendingInvertedIndex {
                index_name: self.index_name,
                file: PendingIndexFile {
                    location: self.location,
                    data,
                },
            }],
            ..Default::default()
        })
    }
}

struct InvertedIndexLowLevelWriter {
    index_name: String,
    location: Location,
    field_indexes: Vec<usize>,
    columns: Vec<Option<Vec<Column>>>,
    writer: Option<InvertedIndexWriter>,
    write: Option<OpenDalBlockingWrite>,
    next_field: usize,
    num_fields: usize,
}

impl BlockIndexLowLevelWriter for InvertedIndexLowLevelWriter {
    fn next_column(mut self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelColumnWriter>> {
        if self.next_field >= self.num_fields {
            return Err(ErrorCode::Internal(
                "inverted index low-level writer has no remaining columns",
            ));
        }
        let field_index = self.next_field;
        self.next_field += 1;
        let fragments = self
            .field_indexes
            .contains(&field_index)
            .then(Vec::<Column>::new);
        Ok(Box::new(InvertedIndexLowLevelColumnWriter {
            parent: Some(self),
            field_index,
            fragments,
        }))
    }

    fn finish(mut self: Box<Self>) -> Result<WrittenBlockIndexOutput> {
        if self.next_field != self.num_fields {
            return Err(ErrorCode::Internal(format!(
                "inverted index low-level writer consumed {} of {} columns",
                self.next_field, self.num_fields
            )));
        }
        let mut columns = Vec::with_capacity(self.field_indexes.len());
        for field_index in self.field_indexes {
            let fragments = self.columns[field_index]
                .take()
                .ok_or_else(|| ErrorCode::Internal("missing inverted index column"))?;
            columns.push(if fragments.len() == 1 {
                fragments.into_iter().next().unwrap()
            } else {
                Column::concat_columns(fragments.into_iter())?
            });
        }
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| ErrorCode::Internal("inverted index writer was consumed"))?;
        writer.add_columns(&columns)?;
        let write = self
            .write
            .take()
            .ok_or_else(|| ErrorCode::Internal("inverted index blocking output was consumed"))?;
        let size = writer.finalize_to_writer(write)?;
        Ok(WrittenBlockIndexOutput {
            inverted: vec![WrittenInvertedIndex {
                index_name: self.index_name,
                file: WrittenIndexFile {
                    location: self.location,
                    size,
                },
            }],
            ..Default::default()
        })
    }
}

struct InvertedIndexLowLevelColumnWriter {
    parent: Option<Box<InvertedIndexLowLevelWriter>>,
    field_index: usize,
    fragments: Option<Vec<Column>>,
}

impl BlockIndexLowLevelColumnWriter for InvertedIndexLowLevelColumnWriter {
    fn write(&mut self, column: &Column) -> Result<()> {
        if let Some(fragments) = self.fragments.as_mut() {
            fragments.push(column.clone());
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelWriter>> {
        let mut parent = self
            .parent
            .take()
            .ok_or_else(|| ErrorCode::Internal("inverted index column writer has no parent"))?;
        parent.columns[self.field_index] = self.fragments.take();
        Ok(parent)
    }
}

pub fn create_inverted_index_builders(table_meta: &TableMeta) -> Vec<InvertedIndexBuilder> {
    let mut inverted_index_builders = Vec::with_capacity(table_meta.indexes.len());
    for index in table_meta.indexes.values() {
        if !matches!(index.index_type, TableIndexType::Inverted) {
            continue;
        }
        if !index.sync_creation {
            continue;
        }
        let mut index_fields = Vec::with_capacity(index.column_ids.len());
        for column_id in &index.column_ids {
            for field in &table_meta.schema.fields {
                if field.column_id() == *column_id {
                    index_fields.push(DataField::from(field));
                    break;
                }
            }
        }
        // ignore invalid index
        if index_fields.len() != index.column_ids.len() {
            debug!(
                "Ignoring invalid inverted index: {}, missing columns",
                index.name
            );
            continue;
        }
        let index_schema = DataSchema::new(index_fields);

        let inverted_index_builder = InvertedIndexBuilder {
            name: index.name.clone(),
            version: index.version.clone(),
            schema: index_schema,
            options: index.options.clone(),
        };
        inverted_index_builders.push(inverted_index_builder);
    }
    inverted_index_builders
}

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
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let field_index = source_schema.index_of(field.name().as_str())?;
                Ok(block.get_by_offset(field_index).to_column())
            })
            .collect::<Result<Vec<_>>>()?;
        self.add_columns(&columns)
    }

    /// Add the indexed columns in this writer's schema order.
    pub fn add_columns(&mut self, columns: &[databend_common_expression::Column]) -> Result<()> {
        if columns.len() != self.schema.num_fields() {
            return Err(ErrorCode::BadArguments(format!(
                "inverted index column count {} != expected {}",
                columns.len(),
                self.schema.num_fields()
            )));
        }
        let rows = columns
            .first()
            .map_or(0, databend_common_expression::Column::len);
        if columns.iter().any(|column| column.len() != rows) {
            return Err(ErrorCode::BadArguments(
                "inverted index columns have different row counts",
            ));
        }

        for row in 0..rows {
            let mut doc = TantivyDocument::new();
            for (index, (field_def, column)) in self.schema.fields().iter().zip(columns).enumerate()
            {
                let field = Field::from_field_id(index as u32);
                let ty = field_def.data_type().remove_nullable();
                match unsafe { column.index_unchecked(row) } {
                    ScalarRef::String(text) => doc.add_text(field, text),
                    ScalarRef::Variant(jsonb_val) => {
                        let raw_jsonb = RawJsonb::new(jsonb_val);
                        if let Ok(value) = from_raw_jsonb::<serde_json::Value>(&raw_jsonb) {
                            if value.is_object() {
                                let owned_value = OwnedValue::from(value);
                                doc.add_field_value(field, &owned_value);
                            } else {
                                // Tantivy only supports object JSON. Wrap other JSON values with an
                                // empty key to preserve the existing index representation.
                                let owned_value = OwnedValue::from(value);
                                let mut wrapped = BTreeMap::new();
                                wrapped.insert("".to_string(), owned_value);
                                doc.add_object(field, wrapped);
                            }
                        } else {
                            doc.add_object(field, BTreeMap::new());
                        }
                    }
                    _ if ty == DataType::Variant => doc.add_object(field, BTreeMap::new()),
                    _ => doc.add_text(field, ""),
                }
            }
            self.operations.push(UserOperation::Add(doc));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub fn finalize_to_writer(mut self, write: OpenDalBlockingWrite) -> Result<u64> {
        let _ = self.index_writer.run(self.operations);
        let _ = self.index_writer.commit()?;
        let index = self.index_writer.index();
        let directory = index.directory();
        let (index_schema, index_block) = build_inverted_index_block(index, directory)?;
        let (_, write) = block_to_parquet_with_writer(
            index_schema.as_ref(),
            index_block,
            TableCompression::Zstd,
            false,
            None,
            write,
        )?;
        Ok(write.bytes_written())
    }

    #[async_backtrace::framed]
    pub fn finalize(mut self) -> Result<Buffer> {
        let _ = self.index_writer.run(self.operations);
        let _ = self.index_writer.commit()?;
        let index = self.index_writer.index();
        let directory = index.directory();

        let (index_schema, index_block) = build_inverted_index_block(index, directory)?;
        let serialized = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            // Zstd has the best compression ratio
            TableCompression::Zstd,
            // No dictionary page for inverted index
            false,
            None,
        )?;

        Ok(Buffer::from(serialized.payload))
    }
}

fn build_inverted_index_block(
    index: &tantivy::Index,
    directory: &dyn Directory,
) -> Result<(TableSchemaRef, DataBlock)> {
    let mut index_columns = Vec::with_capacity(8);

    let managed_filepath = Path::new(".managed.json");
    let managed_bytes = directory.atomic_read(managed_filepath)?;
    let managed_scalar = Scalar::Binary(managed_bytes);
    let managed_block_entry = BlockEntry::new_const_column(DataType::Binary, managed_scalar, 1);
    index_columns.push(managed_block_entry);

    let meta_filepath = Path::new("meta.json");
    let meta_data = directory.atomic_read(meta_filepath)?;
    let meta_string = std::str::from_utf8(&meta_data)?;
    let meta_val: serde_json::Value = serde_json::from_str(meta_string)?;
    let meta_json: String = serde_json::to_string(&meta_val)?;
    let meta_scalar = Scalar::Binary(meta_json.into_bytes());
    let meta_block_entry = BlockEntry::new_const_column(DataType::Binary, meta_scalar, 1);
    index_columns.push(meta_block_entry);

    let segments = index.searchable_segments()?;
    let segment = &segments[0];
    let components = vec![
        SegmentComponent::FastFields,
        SegmentComponent::Store,
        SegmentComponent::FieldNorms,
        SegmentComponent::Positions,
        SegmentComponent::Postings,
        SegmentComponent::Terms,
    ];
    for component in components {
        let component_field = segment.open_read(component)?;
        let bytes = component_field.read_bytes()?;
        let mut value = bytes.as_slice().to_vec();
        let footer = build_tantivy_footer(&value)?;
        value.extend_from_slice(&footer);

        let scalar = Scalar::Binary(value);
        let block_entry = BlockEntry::new_const_column(DataType::Binary, scalar, 1);
        index_columns.push(block_entry);
    }

    let index_fields = vec![
        TableField::new(".managed.json", TableDataType::Binary),
        TableField::new("meta.json", TableDataType::Binary),
        TableField::new("fast", TableDataType::Binary),
        TableField::new("store", TableDataType::Binary),
        TableField::new("fieldnorm", TableDataType::Binary),
        TableField::new("pos", TableDataType::Binary),
        TableField::new("idx", TableDataType::Binary),
        TableField::new("term", TableDataType::Binary),
    ];

    let index_schema = TableSchemaRefExt::create(index_fields);
    let index_block = DataBlock::new(index_columns, 1);
    Ok((index_schema, index_block))
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
        let chinese_analyzer = TextAnalyzer::builder(JiebaTokenizer::new())
            .filter(LowerCaser)
            .build();

        (english_analyzer, chinese_analyzer)
    } else {
        let mut english_analyzer =
            TextAnalyzer::builder(SimpleTokenizer::default()).filter_dynamic(LowerCaser);
        let mut chinese_analyzer =
            TextAnalyzer::builder(JiebaTokenizer::new()).filter_dynamic(LowerCaser);

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
    let json_options = JsonObjectOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_fast(None);

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
