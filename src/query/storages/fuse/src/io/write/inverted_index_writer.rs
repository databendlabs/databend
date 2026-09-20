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
use std::sync::LazyLock;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_storages_common_index::BundleSizes;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexBundleBuilder;
use databend_storages_common_index::InvertedIndexOutputDirectory;
use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::BlockingWrite;
use databend_storages_common_io::create_blocking_write;
use databend_storages_common_table_meta::meta::Location;
use jsonb::RawJsonb;
use jsonb::from_raw_jsonb;
use lindera::dictionary::Dictionary;
use lindera::dictionary::load_dictionary;
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;
use lindera_analysis::token_filter::BoxTokenFilter;
use lindera_analysis::token_filter::japanese_base_form::JapaneseBaseFormTokenFilter;
use lindera_analysis::token_filter::japanese_stop_tags::JapaneseStopTagsTokenFilter;
use lindera_tantivy::tokenizer::LinderaTokenizer;
use log::debug;
use log::info;
use opendal::Operator;
use tantivy::IndexBuilder;
use tantivy::IndexSettings;
use tantivy::SingleSegmentIndexWriter;
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

use crate::io::TableMetaLocationGenerator;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::WrittenInvertedIndex;

static JAPANESE_DICTIONARY: LazyLock<Dictionary> = LazyLock::new(|| {
    load_dictionary("embedded://ipadic").expect("the embedded IPADIC dictionary must be available")
});

#[derive(Clone)]
pub struct InvertedIndexBuilder {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) schema: DataSchema,
    pub(crate) options: BTreeMap<String, String>,
}

impl InvertedIndexBuilder {
    pub fn gen_inverted_index_location(
        &self,
        location_generator: &TableMetaLocationGenerator,
    ) -> String {
        location_generator.gen_inverted_index_v2_location(&self.version)
    }

    /// Binds this index definition to a freshly generated immutable object location.
    ///
    /// Inverted index object keys are independent from the block key, so the location is
    /// resolved once per block write and carried by the spec, matching the other index specs.
    pub(crate) fn into_write_spec(
        self,
        location_generator: &TableMetaLocationGenerator,
    ) -> InvertedIndexWriteSpec {
        let location = (
            self.gen_inverted_index_location(location_generator),
            INVERTED_INDEX_FILE_FORMAT_VERSION,
        );
        InvertedIndexWriteSpec {
            builder: self,
            location,
        }
    }
}

pub(crate) struct InvertedIndexWriteSpec {
    builder: InvertedIndexBuilder,
    location: Location,
}

impl BlockIndexSpec for InvertedIndexWriteSpec {
    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>> {
        Ok(Box::new(InvertedIndexBlockWriter {
            index_name: self.builder.name.clone(),
            index_version: self.builder.version.clone(),
            location: self.location.clone(),
            source_schema: context.physical_schema,
            writer: InvertedIndexWriter::try_create(
                Arc::new(self.builder.schema.clone()),
                &self.builder.options,
                context.operator,
                self.location.0.clone(),
            )?,
        }))
    }
}

struct InvertedIndexBlockWriter {
    index_name: String,
    index_version: String,
    location: Location,
    source_schema: TableSchemaRef,
    writer: InvertedIndexWriter,
}

impl BlockIndexWriter for InvertedIndexBlockWriter {
    fn write(&mut self, block: &DataBlock) -> Result<()> {
        self.writer.add_block(&self.source_schema, block)
    }

    fn finish(self: Box<Self>) -> Result<PendingBlockIndexOutput> {
        let start = Instant::now();
        info!(
            "Start build inverted index for location: {}",
            self.location.0
        );
        let sizes = self.writer.finalize()?;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let total_size = sizes.bundle + sizes.siblings;
        metrics_inc_block_inverted_index_generate_milliseconds(elapsed_ms);
        metrics_inc_block_inverted_index_write_nums(1);
        metrics_inc_block_inverted_index_write_bytes(total_size);
        metrics_inc_block_inverted_index_write_milliseconds(elapsed_ms);
        info!(
            "Finish build inverted index: location={}, bundle={} bytes, siblings={} bytes in {} ms",
            self.location.0, sizes.bundle, sizes.siblings, elapsed_ms
        );
        Ok(PendingBlockIndexOutput {
            inverted: vec![WrittenInvertedIndex {
                index_name: self.index_name,
                index_version: self.index_version,
                location: self.location,
                bundle_size: sizes.bundle,
                total_size,
            }],
            ..Default::default()
        })
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

/// `SingleSegmentIndexWriter` uses its budget only to size the initial term hash table, capped at
/// 2^19 entries (4 MiB); anything above ~12 MiB reaches that cap. The arena itself grows on demand.
const INDEX_WRITER_TABLE_SIZING_HINT: usize = 16 * 1024 * 1024;

pub struct InvertedIndexWriter {
    schema: DataSchemaRef,
    /// Tantivy fields in `schema` order, as assigned by `create_index_schema`.
    index_fields: Vec<Field>,
    operator: Operator,
    location: String,
    directory: InvertedIndexOutputDirectory,
    /// Indexes on the calling thread into exactly one segment: no worker or merge threads, and
    /// no memory-triggered segment split, which matters because Databend reads Tantivy doc ids
    /// as block row numbers.
    index_writer: SingleSegmentIndexWriter,
}

impl InvertedIndexWriter {
    pub fn try_create(
        schema: DataSchemaRef,
        index_options: &BTreeMap<String, String>,
        operator: Operator,
        location: String,
    ) -> Result<InvertedIndexWriter> {
        let directory = InvertedIndexOutputDirectory::new(operator.clone(), location.clone());
        Self::try_create_into(schema, index_options, operator, location, directory)
    }

    #[cfg(test)]
    pub(crate) fn try_create_with_stream_threshold(
        schema: DataSchemaRef,
        index_options: &BTreeMap<String, String>,
        operator: Operator,
        location: String,
        stream_threshold: usize,
    ) -> Result<InvertedIndexWriter> {
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            location.clone(),
            stream_threshold,
        );
        Self::try_create_into(schema, index_options, operator, location, directory)
    }

    fn try_create_into(
        schema: DataSchemaRef,
        index_options: &BTreeMap<String, String>,
        operator: Operator,
        location: String,
        directory: InvertedIndexOutputDirectory,
    ) -> Result<InvertedIndexWriter> {
        let (index_schema, index_fields) = create_index_schema(schema.clone(), index_options)?;

        // No field is stored, so the doc store only holds empty documents; compressing them
        // inline is negligible and avoids one compression thread per block index.
        let index_settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        };

        let tokenizer_manager = create_tokenizer_manager(index_options);

        let index_builder = IndexBuilder::new()
            .settings(index_settings)
            .schema(index_schema.clone())
            .tokenizers(tokenizer_manager.clone());

        let index = index_builder.open_or_create(directory.clone())?;
        let index_writer = SingleSegmentIndexWriter::new(index, INDEX_WRITER_TABLE_SIZING_HINT)?;

        Ok(Self {
            schema,
            index_fields,
            operator,
            location,
            directory,
            index_writer,
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
            for (field, (field_index, ty)) in self.index_fields.iter().zip(&field_indexes) {
                let field = *field;
                let column = block.get_by_offset(*field_index);
                match unsafe { column.index_unchecked(i) } {
                    ScalarRef::String(text) => doc.add_text(field, text),
                    ScalarRef::Variant(jsonb_val) => {
                        let raw_jsonb = RawJsonb::new(jsonb_val);
                        if let Ok(value) = from_raw_jsonb::<serde_json::Value>(&raw_jsonb) {
                            if value.is_object() {
                                let owned_value = OwnedValue::from(value);
                                doc.add_field_value(field, &owned_value);
                            } else {
                                // tantivy only support object JSON,
                                // convert other JSON to object with an empty key.
                                let owned_value = OwnedValue::from(value);
                                let mut wrap_owned_value = BTreeMap::new();
                                wrap_owned_value.insert("".to_string(), owned_value);
                                doc.add_object(field, wrap_owned_value);
                            }
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
            self.index_writer.add_document(doc)?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub fn finalize(self) -> Result<BundleSizes> {
        let index = self.index_writer.finalize()?;
        let builder = InvertedIndexBundleBuilder::try_create(self.directory, index)?;
        let mut sink =
            create_blocking_write(self.operator, self.location, BLOCKING_WRITE_MAX_CHUNKS);
        let sizes = builder.write_to(&mut sink)?;
        sink.close()?;
        Ok(sizes)
    }
}

// Create tokenizers for English, Chinese, and Japanese.
pub(crate) fn create_tokenizer_manager(
    index_options: &BTreeMap<String, String>,
) -> TokenizerManager {
    let tokenizer_manager = TokenizerManager::new();
    let filters = index_options
        .get("filters")
        .map(|filters| filters.split(',').collect::<HashSet<_>>())
        .unwrap_or_default();

    let tokenizer = index_options
        .get("tokenizer")
        .map(String::as_str)
        .unwrap_or("english");
    match tokenizer {
        "english" => tokenizer_manager.register("english", create_english_analyzer(&filters)),
        "chinese" => tokenizer_manager.register("chinese", create_chinese_analyzer(&filters)),
        "japanese" => tokenizer_manager.register("japanese", create_japanese_analyzer(&filters)),
        _ => unreachable!("Invalid tokenizer {}", tokenizer),
    }

    tokenizer_manager
}

fn create_english_analyzer(filters: &HashSet<&str>) -> TextAnalyzer {
    let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default()).filter_dynamic(LowerCaser);

    if filters.contains("english_stop") {
        analyzer = analyzer.filter_dynamic(StopWordFilter::new(Language::English).unwrap());
    }
    if filters.contains("english_stemmer") {
        analyzer = analyzer.filter_dynamic(Stemmer::new(Language::English));
    }

    analyzer.build()
}

fn create_chinese_analyzer(filters: &HashSet<&str>) -> TextAnalyzer {
    let mut analyzer = TextAnalyzer::builder(JiebaTokenizer::new()).filter_dynamic(LowerCaser);

    if filters.contains("english_stop") {
        analyzer = analyzer.filter_dynamic(StopWordFilter::new(Language::English).unwrap());
    }
    if filters.contains("english_stemmer") {
        analyzer = analyzer.filter_dynamic(Stemmer::new(Language::English));
    }
    if filters.contains("chinese_stop") {
        // Punctuation tokens copied from Lucene's Smart Chinese Analyzer.
        // https://github.com/apache/lucene/blob/main/lucene/analysis/smartcn/src/resources/org/apache/lucene/analysis/cn/smart/stopwords.txt
        analyzer = analyzer.filter_dynamic(StopWordFilter::remove(chinese_stop_words()));
    }

    analyzer.build()
}

fn create_japanese_analyzer(filters: &HashSet<&str>) -> TextAnalyzer {
    let segmenter = Segmenter::new(Mode::Normal, JAPANESE_DICTIONARY.clone(), None);
    let mut tokenizer = LinderaTokenizer::from_segmenter(segmenter);

    // Lindera filters must run before conversion to Tantivy tokens because
    // part-of-speech and base-form details are not retained by Tantivy.
    if filters.contains("japanese_stemmer") {
        tokenizer.append_token_filter(BoxTokenFilter::from(JapaneseBaseFormTokenFilter::new()));
    }
    if filters.contains("japanese_stop") {
        tokenizer.append_token_filter(BoxTokenFilter::from(JapaneseStopTagsTokenFilter::new(
            japanese_stop_tags(),
        )));
    }

    let mut analyzer = TextAnalyzer::builder(tokenizer).filter_dynamic(LowerCaser);
    if filters.contains("english_stop") {
        analyzer = analyzer.filter_dynamic(StopWordFilter::new(Language::English).unwrap());
    }
    if filters.contains("english_stemmer") {
        analyzer = analyzer.filter_dynamic(Stemmer::new(Language::English));
    }

    analyzer.build()
}

fn chinese_stop_words() -> Vec<String> {
    [
        ",", ".", "`", "-", "_", "=", "?", "'", "|", "\"", "(", ")", "{", "}", "[", "]", "<", ">",
        "*", "#", "&", "^", "$", "@", "!", "~", ":", ";", "+", "/", "\\", "《", "》", "—", "－",
        "，", "。", "、", "：", "；", "！", "·", "？", "“", "”", "）", "（", "【", "】", "［",
        "］", "●", "　",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn japanese_stop_tags() -> HashSet<String> {
    [
        "接続詞",
        "助詞",
        "助詞,格助詞",
        "助詞,格助詞,一般",
        "助詞,格助詞,引用",
        "助詞,格助詞,連語",
        "助詞,係助詞",
        "助詞,副助詞",
        "助詞,間投助詞",
        "助詞,並立助詞",
        "助詞,終助詞",
        "助詞,副助詞／並立助詞／終助詞",
        "助詞,連体化",
        "助詞,副詞化",
        "助詞,特殊",
        "助動詞",
        "記号",
        "記号,一般",
        "記号,読点",
        "記号,句点",
        "記号,空白",
        "記号,括弧閉",
        "その他,間投",
        "フィラー",
        "非言語音",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
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
    // Tantivy executes JSON range queries over fast fields. The remote reader warms the segment's
    // `.fast` file asynchronously before starting synchronous search.
    let json_options = JsonObjectOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_fast(Some("raw"));

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

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::StringType;
    use databend_storages_common_index::BundleSizes;
    use opendal::services::Memory;
    use tantivy::Term;
    use tantivy::query::Query;
    use tantivy::query::TermQuery;

    use super::*;
    use crate::io::read::InvertedIndexReader;
    use crate::io::read::InvertedIndexWarmupInfo;
    use crate::test_utils::init_test_globals;

    const ROWS: usize = 4000;
    const WORDS: [&str; 5] = ["alpha", "bravo", "charlie", "delta", "echo"];

    /// Row `i` mentions `WORDS[i % 5]` and `WORDS[i % 3]`.
    fn body(i: usize) -> String {
        format!("row {i} talks about {} and {}", WORDS[i % 5], WORDS[i % 3])
    }

    fn expected_rows(word: &str) -> Vec<usize> {
        let mut rows = Vec::new();
        for i in 0..ROWS {
            if WORDS[i % 5] == word || WORDS[i % 3] == word {
                rows.push(i);
            }
        }
        rows
    }

    fn index_options() -> BTreeMap<String, String> {
        BTreeMap::from([("tokenizer".to_string(), "english".to_string())])
    }

    fn build_index(operator: &Operator, location: &str, stream_threshold: usize) -> BundleSizes {
        let data_schema = Arc::new(DataSchema::new(vec![DataField::new(
            "body",
            DataType::String,
        )]));
        let source_schema = Arc::new(TableSchema::new(vec![TableField::new(
            "body",
            TableDataType::String,
        )]));
        let mut writer = InvertedIndexWriter::try_create_with_stream_threshold(
            data_schema,
            &index_options(),
            operator.clone(),
            location.to_string(),
            stream_threshold,
        )
        .unwrap();
        let mut texts = Vec::with_capacity(ROWS);
        for i in 0..ROWS {
            texts.push(body(i));
        }
        let block = DataBlock::new_from_columns(vec![StringType::from_data(texts)]);
        writer.add_block(&source_schema, &block).unwrap();
        writer.finalize().unwrap()
    }

    async fn search(
        operator: &Operator,
        location: &str,
        bundle_size: u64,
        word: &str,
    ) -> Vec<usize> {
        let field = Field::from_field_id(0);
        let query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(field, word),
            IndexRecordOption::Basic,
        ));
        let warmup = InvertedIndexWarmupInfo::try_create(query.as_ref(), &[field]).unwrap();
        let reader = InvertedIndexReader::create(
            operator.clone(),
            false,
            create_tokenizer_manager(&index_options()),
            warmup,
        );
        let result = reader
            .do_filter(
                query,
                location,
                INVERTED_INDEX_FILE_FORMAT_VERSION,
                bundle_size,
                ROWS as u64,
            )
            .await
            .unwrap();
        let (mut rows, _) = result.unwrap_or_default();
        rows.sort_unstable();
        rows
    }

    async fn exists(operator: &Operator, path: &str) -> bool {
        operator.exists(path).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_streamed_and_inline_bundles_answer_the_same_queries() {
        init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();

        let streamed = build_index(&operator, "t/streamed.index", 256);
        let inline = build_index(&operator, "t/inline.index", usize::MAX);

        assert!(exists(&operator, "t/streamed.index.idx").await);
        assert!(exists(&operator, "t/streamed.index.pos").await);
        assert!(streamed.siblings > 0);
        assert!(!exists(&operator, "t/inline.index.idx").await);
        assert!(!exists(&operator, "t/inline.index.pos").await);
        assert_eq!(inline.siblings, 0);
        assert!(streamed.bundle < inline.bundle);
        assert_eq!(
            operator
                .stat("t/streamed.index")
                .await
                .unwrap()
                .content_length(),
            streamed.bundle
        );

        for word in WORDS {
            let expected = expected_rows(word);
            assert_eq!(
                search(&operator, "t/streamed.index", streamed.bundle, word).await,
                expected,
                "streamed {word}"
            );
            assert_eq!(
                search(&operator, "t/inline.index", inline.bundle, word).await,
                expected,
                "inline {word}"
            );
        }
        assert!(
            search(&operator, "t/streamed.index", streamed.bundle, "zulu")
                .await
                .is_empty()
        );
    }
}
