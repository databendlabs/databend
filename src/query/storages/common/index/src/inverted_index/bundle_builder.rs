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
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use tantivy::Directory;
use tantivy::Index;

use super::bundle::BundleExternalFiles;
use super::bundle::BundleFileRanges;
use super::bundle::InvertedIndexBundleFooter;
use super::bundle::MANAGED_JSON_PATH;
use super::bundle::META_JSON_PATH;
use super::directory::collect_index_open_slices;
use super::output_directory::InvertedIndexOutputDirectory;

pub struct BundleSizes {
    /// Bytes of the bundle object itself.
    pub bundle: u64,
    /// Bytes of every sibling object referenced by the bundle.
    pub siblings: u64,
}

/// Packs one committed single-segment index into the bundle object: inline files in lookup
/// priority order, then the footer and trailer. Streamed files are only referenced.
pub struct InvertedIndexBundleBuilder {
    directory: InvertedIndexOutputDirectory,
    index: Index,
}

impl InvertedIndexBundleBuilder {
    pub fn try_create(
        directory: InvertedIndexOutputDirectory,
        index: Index,
    ) -> tantivy::Result<Self> {
        let segments = index.load_metas()?.segments.len();
        if segments != 1 {
            let message =
                format!("inverted index bundle expects one Tantivy segment, got {segments}");
            return Err(io::Error::new(io::ErrorKind::InvalidData, message).into());
        }
        Ok(Self { directory, index })
    }

    pub fn write_to<W: Write>(self, sink: &mut W) -> tantivy::Result<BundleSizes> {
        // Opaque ranges Tantivy reads while opening the index; Databend never interprets them.
        let open_slices = collect_index_open_slices(self.directory.clone())?;
        let ram = self.directory.ram_directory();
        let managed_json = ram.atomic_read(Path::new(MANAGED_JSON_PATH))?;
        let meta_json = ram.atomic_read(Path::new(META_JSON_PATH))?;
        let external_files = self.directory.external_files();

        // ManagedDirectory can briefly retain stale paths, so only include files that still exist.
        let mut paths = Vec::new();
        for path in self.index.directory().list_managed_files() {
            let index_level =
                path == Path::new(MANAGED_JSON_PATH) || path == Path::new(META_JSON_PATH);
            if !index_level && !external_files.contains_key(&path) && ram.exists(&path)? {
                paths.push(path);
            }
        }
        Self::sort_bundle_paths(&mut paths);

        let mut file_ranges = BTreeMap::new();
        let mut written = 0u64;
        for path in paths {
            let bytes = ram.open_read(&path)?.read_bytes()?;
            sink.write_all(&bytes)?;
            let end = written + bytes.len() as u64;
            file_ranges.insert(path, written..end);
            written = end;
        }

        let mut siblings = 0u64;
        for file in external_files.values() {
            siblings += file.len;
        }
        let footer_and_trailer = InvertedIndexBundleFooter::encode_footer_and_trailer(
            BundleFileRanges { files: file_ranges },
            BundleExternalFiles {
                files: external_files,
            },
            open_slices,
            managed_json,
            meta_json,
            written,
        )?;
        sink.write_all(&footer_and_trailer)?;
        Ok(BundleSizes {
            bundle: written + footer_and_trailer.len() as u64,
            siblings,
        })
    }

    /// Small, high-reuse lookup components go last so the normal 1 MiB tail read brings them in
    /// with the footer. Ordering is deterministic within each priority.
    fn sort_bundle_paths(paths: &mut [PathBuf]) {
        paths.sort_unstable_by(|left, right| {
            let by_priority =
                Self::bundle_path_priority(left).cmp(&Self::bundle_path_priority(right));
            by_priority.then_with(|| left.cmp(right))
        });
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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::InvertedIndexBundleBuilder;

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
        InvertedIndexBundleBuilder::sort_bundle_paths(&mut paths);
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

#[cfg(test)]
mod streaming_tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::Operator;
    use opendal::services::Memory;
    use tantivy::Index;
    use tantivy::IndexSettings;
    use tantivy::SingleSegmentIndexWriter;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::JsonObjectOptions;
    use tantivy::schema::OwnedValue;
    use tantivy::schema::Schema;
    use tantivy::schema::TantivyDocument;
    use tantivy::schema::TextFieldIndexing;
    use tantivy::schema::TextOptions;

    use super::InvertedIndexBundleBuilder;
    use crate::inverted_index::InvertedIndexBundleFooter;
    use crate::inverted_index::InvertedIndexOutputDirectory;

    fn object_len(operator: &Operator, path: &str) -> u64 {
        GlobalIORuntime::instance()
            .block_on(async { Ok(operator.stat(path).await?.content_length()) })
            .unwrap()
    }

    fn build_streamed(
        threshold: usize,
        rows: usize,
    ) -> (Operator, InvertedIndexOutputDirectory, Vec<u8>, u64, u64) {
        crate::init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            "t/h1.index".into(),
            threshold,
        );
        let indexing = TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let mut builder = Schema::builder();
        let title = builder.add_text_field(
            "title",
            TextOptions::default().set_indexing_options(indexing.clone()),
        );
        let meta = builder.add_json_field(
            "meta",
            JsonObjectOptions::default()
                .set_indexing_options(indexing)
                .set_fast(Some("raw")),
        );
        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        };
        let index = Index::create(directory.clone(), builder.build(), settings).unwrap();
        let mut writer = SingleSegmentIndexWriter::new(index, 16 * 1024 * 1024).unwrap();
        for i in 0..rows {
            let mut doc = TantivyDocument::new();
            doc.add_text(
                title,
                format!("alpha beta gamma{} delta epsilon{}", i % 17, i),
            );
            let json = serde_json::json!({ "tag": format!("t{}", i % 5), "n": i, "nested": { "s": "x y z" } });
            doc.add_field_value(meta, &OwnedValue::from(json));
            writer.add_document(doc).unwrap();
        }
        let index = writer.finalize().unwrap();
        let mut bundle = Vec::new();
        let sizes = InvertedIndexBundleBuilder::try_create(directory.clone(), index)
            .unwrap()
            .write_to(&mut bundle)
            .unwrap();
        (operator, directory, bundle, sizes.bundle, sizes.siblings)
    }

    #[test]
    fn test_streamed_bundle_open_slices_fit_in_retained_tail() {
        const THRESHOLD: usize = 4096;
        let (operator, directory, bundle, bundle_size, siblings) = build_streamed(THRESHOLD, 3000);
        assert_eq!(bundle_size as usize, bundle.len());
        let footer = InvertedIndexBundleFooter::open(&bundle).unwrap();

        let external = directory.external_files();
        assert_eq!(
            external.len(),
            2,
            "both .idx and .pos should have streamed: {external:?}"
        );
        let mut sibling_total = 0;
        for (path, file) in &external {
            assert!(!footer.file_ranges.contains(path));
            assert_eq!(footer.external_files.get(path), Some(file));
            assert_eq!(
                object_len(&operator, &format!("t/h1.index{}", file.suffix)),
                file.len
            );
            assert!(file.len as usize > THRESHOLD);
            sibling_total += file.len;
        }
        assert_eq!(siblings, sibling_total);
        for component in ["term", "store", "fast", "fieldnorm"] {
            assert!(
                footer
                    .file_ranges
                    .files
                    .keys()
                    .any(|p| p.extension().is_some_and(|e| e == component)),
                "{component}"
            );
        }

        for (path, file) in &external {
            let slices = &footer.open_slices[path];
            let tail_start = file.len - THRESHOLD as u64;
            let mut slice_bytes = 0u64;
            let mut outside_tail = 0u64;
            for slice in slices.iter() {
                assert!(slice.range.end <= file.len);
                slice_bytes += slice.range.end - slice.range.start;
                if slice.range.start < tail_start {
                    outside_tail += slice.range.end - slice.range.start;
                }
            }
            assert!(
                outside_tail <= 16,
                "{}: {outside_tail} bytes read outside the tail",
                path.display()
            );
            assert!(
                slice_bytes < 512,
                "{}: open reads {slice_bytes} bytes of a {} byte file",
                path.display(),
                file.len
            );
        }
    }

    #[test]
    fn test_small_index_stays_single_object() {
        let (operator, directory, bundle, _, siblings) = build_streamed(1 << 20, 50);
        assert!(directory.external_files().is_empty());
        assert_eq!(siblings, 0);
        let footer = InvertedIndexBundleFooter::open(&bundle).unwrap();
        assert!(
            footer
                .file_ranges
                .files
                .keys()
                .any(|p| p.extension().is_some_and(|e| e == "idx"))
        );
        let exists = GlobalIORuntime::instance()
            .block_on(async { Ok(operator.exists("t/h1.index.idx").await?) })
            .unwrap();
        assert!(!exists);
    }
}
