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
use std::path::PathBuf;
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
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_index::MANAGED_JSON_PATH;
use databend_storages_common_index::META_JSON_PATH;
use databend_storages_common_index::collect_index_open_slices;
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
use opendal::Buffer;
use tantivy::Directory;
use tantivy::IndexBuilder;
use tantivy::IndexSettings;
use tantivy::IndexWriter;
use tantivy::directory::RamDirectory;
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

use crate::io::TableMetaLocationGenerator;

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

#[derive(Debug)]
pub struct InvertedIndexState {
    pub(crate) data: Buffer,
    pub(crate) size: u64,
    pub(crate) location: Location,
    pub(crate) index_name: String,
    pub(crate) index_version: String,
}

impl InvertedIndexState {
    pub fn try_create(
        data: Buffer,
        location: String,
        index_name: String,
        index_version: String,
    ) -> Result<Self> {
        let size = data.len() as u64;
        Ok(Self {
            data,
            size,
            location: (location, INVERTED_INDEX_FILE_FORMAT_VERSION),
            index_name,
            index_version,
        })
    }

    pub fn from_data_block(
        source_schema: &TableSchemaRef,
        block: &DataBlock,
        location_generator: &TableMetaLocationGenerator,
        inverted_index_builder: &InvertedIndexBuilder,
    ) -> Result<Self> {
        let start = Instant::now();

        let inverted_index_location =
            inverted_index_builder.gen_inverted_index_location(location_generator);

        info!(
            "Start build inverted index for location: {}",
            inverted_index_location
        );

        let mut writer = InvertedIndexWriter::try_create(
            Arc::new(inverted_index_builder.schema.clone()),
            &inverted_index_builder.options,
        )?;
        writer.add_block(source_schema, block)?;
        let data = writer.finalize()?;

        // Perf.
        let size = data.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_inverted_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build inverted index: location={}, size={} bytes in {} ms",
            inverted_index_location, size, elapsed_ms
        );

        Self::try_create(
            data,
            inverted_index_location,
            inverted_index_builder.name.clone(),
            inverted_index_builder.version.clone(),
        )
    }
}

pub struct InvertedIndexWriter {
    schema: DataSchemaRef,
    directory: RamDirectory,
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

        let directory = RamDirectory::default();
        let index = index_builder.open_or_create(directory.clone())?;
        let index_writer = index.writer(DEFAULT_BLOCK_BUFFER_SIZE)?;
        let operations = Vec::new();

        Ok(Self {
            schema,
            directory,
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
            self.operations.push(UserOperation::Add(doc));
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub fn finalize(mut self) -> Result<Buffer> {
        self.index_writer.run(self.operations)?;
        self.index_writer.commit()?;
        let raw_directory = self.directory.clone();
        let index = self.index_writer.index();
        let index_meta = index.load_metas()?;
        if index_meta.segments.len() != 1 {
            return Err(ErrorCode::StorageOther(format!(
                "inverted index bundle expects one Tantivy segment, got {}",
                index_meta.segments.len()
            )));
        }

        // Observe the opaque segment ranges Tantivy reads while synchronously opening the index.
        // Databend stores these bytes in the footer without interpreting component internals.
        let open_slices = collect_index_open_slices(raw_directory.clone())?;

        let managed_json = raw_directory.atomic_read(Path::new(MANAGED_JSON_PATH))?;
        let meta_json = raw_directory.atomic_read(Path::new(META_JSON_PATH))?;

        // Preserve every managed segment/plugin file byte-for-byte in the raw region. The two
        // frequently read index-level JSON files live in the footer instead. ManagedDirectory can
        // briefly retain stale paths, so only include files that still exist after the commit.
        let mut paths: Vec<PathBuf> = index
            .directory()
            .list_managed_files()
            .into_iter()
            .filter(|path| {
                path != Path::new(MANAGED_JSON_PATH) && path != Path::new(META_JSON_PATH)
            })
            .collect();
        // Keep small, high-reuse lookup components next to the footer so the normal 1 MiB tail
        // read can populate them without another object request. Preserve deterministic ordering
        // within each component priority.
        sort_bundle_paths(&mut paths);

        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            if raw_directory.exists(&path)? {
                let bytes = raw_directory.atomic_read(&path)?;
                files.push((path, bytes));
            }
        }

        let bundle_bytes =
            InvertedIndexBundleFooter::build(files, open_slices, managed_json, meta_json)?;
        Ok(Buffer::from(bundle_bytes))
    }
}

fn bundle_path_priority(path: &Path) -> u8 {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("store") => 1,
        Some("fast") => 2,
        Some("fieldnorm") => 3,
        Some("term") => 4,
        _ => 0,
    }
}

fn sort_bundle_paths(paths: &mut [PathBuf]) {
    paths.sort_unstable_by(|left, right| {
        bundle_path_priority(left)
            .cmp(&bundle_path_priority(right))
            .then_with(|| left.cmp(right))
    });
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
    use std::path::PathBuf;

    use super::sort_bundle_paths;

    #[test]
    fn test_sort_bundle_paths_places_lookup_components_near_footer() {
        let mut paths = vec![
            PathBuf::from("segment.term"),
            PathBuf::from("segment.pos"),
            PathBuf::from("segment.store"),
            PathBuf::from("segment.idx"),
            PathBuf::from("segment.fieldnorm"),
            PathBuf::from("segment.custom"),
            PathBuf::from("segment.fast"),
        ];

        sort_bundle_paths(&mut paths);

        assert_eq!(paths, vec![
            PathBuf::from("segment.custom"),
            PathBuf::from("segment.idx"),
            PathBuf::from("segment.pos"),
            PathBuf::from("segment.store"),
            PathBuf::from("segment.fast"),
            PathBuf::from("segment.fieldnorm"),
            PathBuf::from("segment.term"),
        ]);
    }
}
