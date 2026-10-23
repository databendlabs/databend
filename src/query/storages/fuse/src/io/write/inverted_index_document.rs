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

//! Tantivy documents backed by Databend columns.
//!
//! `TantivyDocument` copies every text value into its own buffer before indexing. These
//! documents instead share the block's column buffers, so String values are tokenized directly
//! from the column. Variant values still go through the existing JSONB -> serde_json ->
//! OwnedValue conversion, but the converted value is borrowed rather than re-encoded.

use std::sync::Arc;

use databend_common_expression::BlockEntry;
use databend_common_expression::ScalarRef;
use jsonb::RawJsonb;
use jsonb::from_raw_jsonb;
use tantivy::schema::Field;
use tantivy::schema::OwnedValue;
use tantivy::schema::Value;
use tantivy::schema::document::Document;
use tantivy::schema::document::ReferenceValue;
use tantivy::schema::document::ReferenceValueLeaf;

enum IndexColumn {
    Text {
        field: Field,
        entry: BlockEntry,
    },
    Json {
        field: Field,
        entry: BlockEntry,
        slot: usize,
    },
}

/// The indexed columns of one block, shared by all of its row documents.
pub(super) struct BlockDocumentColumns {
    columns: Vec<IndexColumn>,
    json_columns: usize,
    num_rows: usize,
}

impl BlockDocumentColumns {
    /// `columns` holds `(field, column, is_variant)` in the index schema's field order.
    /// Cloning a `BlockEntry` shares the column buffers rather than copying values.
    pub(super) fn new(
        columns: impl IntoIterator<Item = (Field, BlockEntry, bool)>,
        num_rows: usize,
    ) -> Arc<Self> {
        let mut json_columns = 0;
        let columns = columns
            .into_iter()
            .map(|(field, entry, is_variant)| {
                if is_variant {
                    json_columns += 1;
                    IndexColumn::Json {
                        field,
                        entry,
                        slot: json_columns - 1,
                    }
                } else {
                    IndexColumn::Text { field, entry }
                }
            })
            .collect();
        Arc::new(Self {
            columns,
            json_columns,
            num_rows,
        })
    }

    pub(super) fn document(self: &Arc<Self>, row: usize) -> BlockDocument {
        // Makes every later `index_unchecked(row)` sound.
        assert!(row < self.num_rows, "inverted-index row out of bounds");
        let mut json_values = Vec::with_capacity(self.json_columns);
        for column in &self.columns {
            if let IndexColumn::Json { entry, .. } = column {
                // SAFETY: `row < num_rows`, checked above.
                json_values.push(variant_value(unsafe { entry.index_unchecked(row) }));
            }
        }
        BlockDocument {
            columns: self.clone(),
            row,
            json_values,
        }
    }
}

/// Convert a Variant cell exactly as the previous `TantivyDocument` path did: Tantivy only
/// indexes JSON objects, so scalars/arrays are wrapped under an empty key, while NULL and
/// invalid JSONB become an empty object.
fn variant_value(scalar: ScalarRef<'_>) -> OwnedValue {
    let ScalarRef::Variant(jsonb) = scalar else {
        return OwnedValue::Object(Vec::new());
    };
    match from_raw_jsonb::<serde_json::Value>(&RawJsonb::new(jsonb)) {
        Ok(value) if value.is_object() => OwnedValue::from(value),
        Ok(value) => OwnedValue::Object(vec![(String::new(), OwnedValue::from(value))]),
        Err(_) => OwnedValue::Object(Vec::new()),
    }
}

/// One block row. It owns a reference to the shared columns, satisfying `Document: 'static`;
/// the values it yields borrow from it for the duration of `add_document()`.
pub(crate) struct BlockDocument {
    columns: Arc<BlockDocumentColumns>,
    row: usize,
    json_values: Vec<OwnedValue>,
}

impl Document for BlockDocument {
    type Value<'a> = IndexValue<'a>;
    type FieldsValuesIter<'a> = BlockFieldValues<'a>;

    // Tantivy may call this more than once per document, so it must only read.
    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        BlockFieldValues {
            doc: self,
            column: 0,
        }
    }
}

pub(crate) struct BlockFieldValues<'a> {
    doc: &'a BlockDocument,
    column: usize,
}

impl<'a> Iterator for BlockFieldValues<'a> {
    type Item = (Field, IndexValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.doc.columns.columns.get(self.column)?;
        self.column += 1;
        Some(match column {
            IndexColumn::Text { field, entry } => {
                // SAFETY: `row < num_rows`, checked when the document was created.
                let text = match unsafe { entry.index_unchecked(self.doc.row) } {
                    ScalarRef::String(text) => text,
                    // NULL text is indexed as an empty string, as before.
                    _ => "",
                };
                (*field, IndexValue::Text(text))
            }
            IndexColumn::Json { field, slot, .. } => {
                (*field, IndexValue::Json(&self.doc.json_values[*slot]))
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.doc.columns.columns.len() - self.column;
        (remaining, Some(remaining))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum IndexValue<'a> {
    /// Borrowed directly from a String column.
    Text(&'a str),
    /// Borrowed from the document's converted Variant values.
    Json(&'a OwnedValue),
}

type JsonArrayIter<'a> =
    std::iter::Map<std::slice::Iter<'a, OwnedValue>, fn(&'a OwnedValue) -> IndexValue<'a>>;
type JsonObjectIter<'a> = std::iter::Map<
    std::slice::Iter<'a, (String, OwnedValue)>,
    fn(&'a (String, OwnedValue)) -> (&'a str, IndexValue<'a>),
>;

fn json_entry<'a>(entry: &'a (String, OwnedValue)) -> (&'a str, IndexValue<'a>) {
    (entry.0.as_str(), IndexValue::Json(&entry.1))
}

impl<'a> Value<'a> for IndexValue<'a> {
    type ArrayIter = JsonArrayIter<'a>;
    type ObjectIter = JsonObjectIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match *self {
            IndexValue::Text(text) => ReferenceValue::Leaf(ReferenceValueLeaf::Str(text)),
            IndexValue::Json(OwnedValue::Array(values)) => ReferenceValue::Array(
                values
                    .iter()
                    .map(IndexValue::Json as fn(&'a OwnedValue) -> IndexValue<'a>),
            ),
            IndexValue::Json(OwnedValue::Object(entries)) => ReferenceValue::Object(
                entries
                    .iter()
                    .map(json_entry as fn(&'a (String, OwnedValue)) -> (&'a str, IndexValue<'a>)),
            ),
            IndexValue::Json(value) => match value.as_value() {
                ReferenceValue::Leaf(leaf) => ReferenceValue::Leaf(leaf),
                ReferenceValue::Array(_) | ReferenceValue::Object(_) => {
                    unreachable!("arrays and objects are handled above")
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::VariantType;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    use super::*;

    fn to_owned(value: IndexValue<'_>) -> OwnedValue {
        match value {
            IndexValue::Text(text) => OwnedValue::Str(text.to_string()),
            IndexValue::Json(value) => value.clone(),
        }
    }

    #[test]
    fn test_document_values_match_previous_conversion() {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let meta = schema.add_json_field("meta", TEXT);
        let object = jsonb::parse_value(br#"{"n":1,"tag":"x"}"#)
            .unwrap()
            .to_vec();
        let scalar = jsonb::parse_value(br#""alpha""#).unwrap().to_vec();
        let columns = BlockDocumentColumns::new(
            [
                (
                    body,
                    StringType::from_opt_data(vec![Some("alpha beta"), None, Some(""), None])
                        .into(),
                    false,
                ),
                (
                    meta,
                    VariantType::from_opt_data(vec![
                        Some(object),
                        Some(scalar),
                        Some(vec![0xff_u8, 0xff]),
                        None,
                    ])
                    .into(),
                    true,
                ),
            ],
            4,
        );

        let object = OwnedValue::Object(vec![
            ("n".to_string(), OwnedValue::I64(1)),
            ("tag".to_string(), OwnedValue::Str("x".to_string())),
        ]);
        let wrapped =
            OwnedValue::Object(vec![(String::new(), OwnedValue::Str("alpha".to_string()))]);
        let empty = OwnedValue::Object(Vec::new());
        let expected = [
            (OwnedValue::Str("alpha beta".to_string()), object),
            (OwnedValue::Str(String::new()), wrapped),
            (OwnedValue::Str(String::new()), empty.clone()),
            (OwnedValue::Str(String::new()), empty),
        ];

        for (row, (text, json)) in expected.into_iter().enumerate() {
            let doc = columns.document(row);
            let values = doc
                .iter_fields_and_values()
                .map(|(field, value)| (field, to_owned(value)))
                .collect::<Vec<_>>();
            assert_eq!(values, vec![(body, text), (meta, json)], "row {row}");
            // Tantivy iterates documents more than once; iteration must be repeatable.
            assert_eq!(doc.iter_fields_and_values().count(), 2);
        }
    }

    #[test]
    #[should_panic(expected = "inverted-index row out of bounds")]
    fn test_document_rejects_out_of_bounds_row() {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let columns =
            BlockDocumentColumns::new([(body, StringType::from_data(vec!["a"]).into(), false)], 1);
        let _ = columns.document(1);
    }
}

/// Equivalence with the previous `TantivyDocument` writer path: both document types must
/// produce the same terms, postings, positions, fieldnorms, JSON fast fields, and scores.
#[cfg(test)]
mod equivalence_tests {
    use std::collections::BTreeMap;

    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::VariantType;
    use tantivy::DocSet;
    use tantivy::Index;
    use tantivy::IndexSettings;
    use tantivy::SingleSegmentIndexWriter;
    use tantivy::TERMINATED;
    use tantivy::TantivyDocument;
    use tantivy::collector::TopDocs;
    use tantivy::postings::Postings;
    use tantivy::query::QueryParser;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::Schema;
    use tantivy::tokenizer::TokenizerManager;

    use super::*;
    use crate::io::write::inverted_index_writer::create_index_schema;
    use crate::io::write::inverted_index_writer::create_tokenizer_manager;

    const ROWS: usize = 600;
    const WORDS: [&str; 5] = ["alpha", "bravo", "charlie", "delta", "echo"];

    /// `(field, column, is_variant)` in index-schema order, as `InvertedIndexWriter` builds them.
    type Columns = Vec<(Field, BlockEntry, bool)>;

    fn body(i: usize) -> Option<String> {
        match i {
            _ if i % 17 == 0 => None,
            _ if i % 13 == 0 => Some(String::new()),
            // Long values exercise multi-block positions.
            _ if i % 50 == 1 => Some(format!("long {} ", WORDS[i % 5]).repeat(300)),
            _ => Some(format!(
                "Row {i} talks about {} and {}, mentioning ÉCOLE café",
                WORDS[i % 5],
                WORDS[i % 3]
            )),
        }
    }

    fn meta(i: usize) -> Option<Vec<u8>> {
        let json = match i % 11 {
            0 => format!(
                r#"{{"tag":"{}","n":{i},"nested":{{"arr":[1,"two",true,null,2.5]}}}}"#,
                WORDS[i % 5]
            ),
            1 => format!(r#"{{"tag":"bravo","n":-{i},"big":18446744073709551615,"f":1.25}}"#),
            2 => r#"{"when":"2024-01-02T03:04:05Z","tag":"gamma delta"}"#.to_string(),
            3 => r#""scalar alpha""#.to_string(),
            4 => r#"[1,2,"three",{"k":"v"}]"#.to_string(),
            5 => "42".to_string(),
            6 => "null".to_string(),
            7 => "true".to_string(),
            8 => "{}".to_string(),
            9 => return Some(vec![0xff, 0xff]), // invalid JSONB
            _ => return None,
        };
        Some(jsonb::parse_value(json.as_bytes()).unwrap().to_vec())
    }

    fn setup() -> (Schema, Columns, TokenizerManager) {
        let options = BTreeMap::from([("tokenizer".to_string(), "english".to_string())]);
        let data_schema = Arc::new(DataSchema::new(vec![
            DataField::new("body", DataType::Nullable(Box::new(DataType::String))),
            DataField::new("title", DataType::String),
            DataField::new("meta", DataType::Nullable(Box::new(DataType::Variant))),
        ]));
        let (schema, fields) = create_index_schema(data_schema, &options).unwrap();
        let bodies = (0..ROWS).map(body).collect::<Vec<_>>();
        let titles = (0..ROWS)
            .map(|i| format!("title {} {}", WORDS[(i + 2) % 5], i % 7))
            .collect::<Vec<_>>();
        let metas = (0..ROWS).map(meta).collect::<Vec<_>>();
        let columns = vec![
            (fields[0], StringType::from_opt_data(bodies).into(), false),
            (fields[1], StringType::from_data(titles).into(), false),
            (fields[2], VariantType::from_opt_data(metas).into(), true),
        ];
        (schema, columns, create_tokenizer_manager(&options, None))
    }

    /// The writer implementation this change replaced, kept verbatim as the reference.
    fn legacy_document(columns: &Columns, row: usize) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        for (field, entry, is_variant) in columns {
            let field = *field;
            // SAFETY: callers pass `row < ROWS`, the length of every column.
            match unsafe { entry.index_unchecked(row) } {
                ScalarRef::String(text) => doc.add_text(field, text),
                ScalarRef::Variant(jsonb_val) => {
                    let raw_jsonb = RawJsonb::new(jsonb_val);
                    if let Ok(value) = from_raw_jsonb::<serde_json::Value>(&raw_jsonb) {
                        if value.is_object() {
                            let owned_value = OwnedValue::from(value);
                            doc.add_field_value(field, &owned_value);
                        } else {
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
                    if *is_variant {
                        doc.add_object(field, BTreeMap::new());
                    } else {
                        doc.add_text(field, "");
                    }
                }
            }
        }
        doc
    }

    fn build<D: Document>(
        schema: &Schema,
        tokenizers: &TokenizerManager,
        docs: impl Iterator<Item = D>,
    ) -> Index {
        let index = Index::builder()
            .settings(IndexSettings {
                docstore_compress_dedicated_thread: false,
                ..Default::default()
            })
            .schema(schema.clone())
            .tokenizers(tokenizers.clone())
            .create_in_ram()
            .unwrap();
        let mut writer = SingleSegmentIndexWriter::<D>::new(index, 16 * 1024 * 1024).unwrap();
        for doc in docs {
            writer.add_document(doc).unwrap();
        }
        writer.finalize().unwrap()
    }

    /// `(term bytes, doc_freq, [(doc, term_freq, positions)])` for every term of a field.
    type FieldPostings = Vec<(Vec<u8>, u32, Vec<(u32, u32, Vec<u32>)>)>;

    #[derive(Debug, PartialEq)]
    struct Snapshot {
        num_docs: u32,
        postings: Vec<FieldPostings>,
        fieldnorms: Vec<Option<Vec<u32>>>,
        /// `(column type, column bytes)` for every fast column under each field.
        fast_columns: Vec<Vec<(String, Vec<u8>)>>,
        /// `(query, [(doc, score bits)])`, sorted by doc.
        scores: Vec<(String, Vec<(u32, u32)>)>,
    }

    fn snapshot(index: &Index, columns: &Columns, tokenizers: &TokenizerManager) -> Snapshot {
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment = &searcher.segment_readers()[0];
        let schema = index.schema();
        let mut snapshot = Snapshot {
            num_docs: segment.num_docs(),
            postings: Vec::new(),
            fieldnorms: Vec::new(),
            fast_columns: Vec::new(),
            scores: Vec::new(),
        };

        for (field, _, _) in columns {
            let inverted = segment.inverted_index(*field).unwrap();
            let mut field_postings = Vec::new();
            let mut terms = inverted.terms().stream().unwrap();
            while terms.advance() {
                let info = terms.value().clone();
                let mut postings = inverted
                    .read_postings_from_terminfo(&info, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                let mut docs = Vec::new();
                while postings.doc() != TERMINATED {
                    let mut positions = Vec::new();
                    postings.positions(&mut positions);
                    docs.push((postings.doc(), postings.term_freq(), positions));
                    postings.advance();
                }
                field_postings.push((terms.key().to_vec(), info.doc_freq, docs));
            }
            snapshot.postings.push(field_postings);

            snapshot
                .fieldnorms
                .push(segment.get_fieldnorms_reader(*field).ok().map(|reader| {
                    (0..segment.max_doc())
                        .map(|doc| reader.fieldnorm(doc))
                        .collect()
                }));

            let name = schema.get_field_name(*field);
            let fast = segment.fast_fields();
            let mut handles = fast.dynamic_column_handles(name).unwrap();
            handles.extend(fast.dynamic_subpath_column_handles(name).unwrap());
            snapshot.fast_columns.push(
                handles
                    .iter()
                    .map(|handle| {
                        let bytes = handle.file_slice().read_bytes().unwrap();
                        (
                            format!("{:?}", handle.column_type()),
                            bytes.as_slice().to_vec(),
                        )
                    })
                    .collect(),
            );
        }

        let fields = columns.iter().map(|(field, _, _)| *field).collect();
        let parser = QueryParser::new(schema, fields, tokenizers.clone());
        for text in [
            "body:alpha",
            "body:\"about alpha\"",
            "body:mentioning OR title:delta",
            "meta.tag:alpha",
            "meta.nested.arr:two",
            "meta.n:[0 TO 100]",
            "meta.tag:bravo",
            "long",
        ] {
            let query = parser.parse_query(text).unwrap();
            let mut docs = searcher
                .search(query.as_ref(), &TopDocs::with_limit(ROWS).order_by_score())
                .unwrap()
                .into_iter()
                .map(|(score, address)| (address.doc_id, score.to_bits()))
                .collect::<Vec<_>>();
            docs.sort_unstable();
            snapshot.scores.push((text.to_string(), docs));
        }
        snapshot
    }

    #[test]
    fn test_block_document_matches_tantivy_document() {
        let (schema, columns, tokenizers) = setup();

        let legacy = build(
            &schema,
            &tokenizers,
            (0..ROWS).map(|row| legacy_document(&columns, row)),
        );
        let block_columns = BlockDocumentColumns::new(columns.clone(), ROWS);
        let current = build(
            &schema,
            &tokenizers,
            (0..ROWS).map(|row| block_columns.document(row)),
        );

        let expected = snapshot(&legacy, &columns, &tokenizers);
        // Guards against a vacuous comparison: the fixture must populate every structure.
        assert_eq!(expected.num_docs, ROWS as u32);
        assert!(expected.postings.iter().all(|terms| !terms.is_empty()));
        assert!(expected.fast_columns[2].len() > 1);
        assert!(expected.scores.iter().all(|(_, docs)| !docs.is_empty()));

        assert_eq!(snapshot(&current, &columns, &tokenizers), expected);
    }
}
