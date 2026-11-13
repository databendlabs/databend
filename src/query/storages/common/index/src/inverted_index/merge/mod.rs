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

mod engine;
mod output;
mod plan;
mod source;
mod streaming_file;
mod window;

pub use engine::MergeOptions;
pub use engine::MergeOutput;
pub use engine::checkpoint_bytes_for;
pub use engine::indexed_fields;
pub use engine::merge_in_process;
pub use engine::plan_index_windows;
pub use engine::scan_source_costs;
pub use output::MergeInterruptCheck;
pub use output::OutputWriteContext;
pub use output::OutputWriter;
pub use plan::FieldCostTable;
pub use plan::TERM_COST_CHECKPOINT_BYTES;
pub use plan::TERM_WINDOW_BYTES;
pub use plan::TermCostCheckpoint;
pub use plan::TermWindow;
pub use plan::plan_field_windows;
pub use plan::scan_field_costs;
pub use source::MergeSource;
pub use source::MergeSourceDirectory;
pub use source::MergeSourceReader;
pub use source::SOURCE_OPEN_TAIL_READ_SIZE;
pub use source::check_schema;
pub use source::json_term_record_option;
pub use source::open_merge_source;
pub use streaming_file::STREAMING_CHUNK_SIZE;
pub use streaming_file::StreamingFileHandle;
pub use streaming_file::StreamingReadStats;
pub use window::OutputSlice;
pub use window::SliceDoc;
pub use window::SliceTerm;
pub use window::merge_window;

pub(super) use crate::index_common::invalid_input as invalid;

#[cfg(test)]
pub(crate) mod test_util {
    use tantivy::DocSet;
    use tantivy::Index;
    use tantivy::TERMINATED;
    use tantivy::postings::Postings;
    use tantivy::schema::FieldType;
    use tantivy::schema::IndexRecordOption;

    use super::source::json_term_record_option;

    /// `(doc id, term frequency, positions)` of one posting.
    pub type Posting = (u32, u32, Vec<u32>);

    /// Everything a merge reads, in the order it reads it: for each field, each term in order
    /// with its postings.
    pub fn walk(index: &Index) -> Vec<(Vec<u8>, Vec<Posting>)> {
        let searcher = index.reader().unwrap().searcher();
        let segment = searcher.segment_reader(0);
        let mut terms = Vec::new();
        for (field, entry) in index.schema().fields() {
            let inverted = segment.inverted_index(field).unwrap();
            let mut stream = inverted.terms().stream().unwrap();
            while stream.advance() {
                let term_info = stream.value().clone();
                let field_option = entry
                    .field_type()
                    .get_index_record_option()
                    .unwrap_or(IndexRecordOption::Basic);
                let option = match entry.field_type() {
                    FieldType::JsonObject(_) => json_term_record_option(field_option, stream.key()),
                    _ => field_option,
                };
                let mut postings = inverted
                    .read_postings_from_terminfo(&term_info, option)
                    .unwrap();
                let mut docs = Vec::new();
                let mut positions = Vec::new();
                while postings.doc() != TERMINATED {
                    let freq = match option {
                        IndexRecordOption::Basic => 1,
                        _ => postings.term_freq(),
                    };
                    positions.clear();
                    if option == IndexRecordOption::WithFreqsAndPositions {
                        postings.positions(&mut positions);
                    }
                    docs.push((postings.doc(), freq, positions.clone()));
                    postings.advance();
                }
                terms.push((stream.key().to_vec(), docs));
            }
        }
        terms
    }
}
