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
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

const BUNDLE_FOOTER_MAGIC: u32 = 0x4442_4946; // DBIF
const BUNDLE_TRAILER_MAGIC: [u8; 4] = *b"DBIV";

pub const MANAGED_JSON_PATH: &str = ".managed.json";
pub const META_JSON_PATH: &str = "meta.json";

/// Outer inverted-index object format stored in `BlockIndexMeta.location.1`.
/// This is the first raw Tantivy bundle object format.
///
/// The footer also stores the opaque byte ranges this Tantivy revision reads while opening an
/// index. Bumping the Tantivy crate without bumping this constant leaves existing objects
/// unreadable: a newly required synchronous range misses the footer and returns `WouldBlock`.
/// A Tantivy upgrade that changes index-open IO therefore requires a new format version and a
/// full `REFRESH TABLE INDEX`.
pub const INVERTED_INDEX_FILE_FORMAT_VERSION: u64 = 1;

/// Fixed trailer: footer start, bundle version, and magic.
pub const INVERTED_INDEX_BUNDLE_TRAILER_LEN: usize = size_of::<u64>() + size_of::<u32>() + 4;

/// Bytes read from the end of an object on the first footer IO.
///
/// This normally includes the complete footer and may also include small lookup components placed
/// immediately before it (`.term`, `.fieldnorm`, and `.fast`). If the footer starts before this
/// tail, readers issue one additional read for the exact persisted footer range reported by the
/// trailer.
///
/// For objects ≤ 1 MiB this first IO is the whole bundle, including `.idx` / `.pos`. Those payload
/// pages are not stored from the tail; a later term warmup may read them again. Repeat queries
/// still only cache the needed 64 KiB payload pages.
pub const INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE: usize = 1024 * 1024;

/// Maximum persisted footer size accepted by readers and writers.
///
/// This is a defensive wire-format limit, not the size of the normal first footer read.
pub const INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE: usize = 4 * 1024 * 1024;
const INVERTED_INDEX_BUNDLE_DECODE_LIMIT: usize = 16 * 1024 * 1024;

/// Independent wire version of the inverted-index bundle footer.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum InvertedIndexBundleVersion {
    /// Raw Tantivy files inline in the bundle body, with large components optionally stored as
    /// sibling objects referenced by suffix.
    #[default]
    V1 = 1,
}

/// Bundle objects end with this; sibling objects append their own suffix after it.
pub const INVERTED_INDEX_BUNDLE_OBJECT_SUFFIX: &str = ".index";

/// A segment file stored as a sibling object `<bundle location><suffix>` instead of inline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalFile {
    pub suffix: String,
    pub len: u64,
}

impl ExternalFile {
    /// Maps any object under the inverted-index prefix back to the bundle it belongs to:
    /// `.../h<uuid>.index` is a bundle, `.../h<uuid>.index.idx` is one of its sibling objects.
    /// Returns `None` for paths that follow neither naming.
    pub fn bundle_location(object_path: &str) -> Option<&str> {
        let position = object_path.rfind(INVERTED_INDEX_BUNDLE_OBJECT_SUFFIX)?;
        let end = position + INVERTED_INDEX_BUNDLE_OBJECT_SUFFIX.len();
        match &object_path[end..] {
            "" => Some(&object_path[..end]),
            suffix if suffix.starts_with('.') && !suffix.contains('/') => Some(&object_path[..end]),
            _ => None,
        }
    }
}

/// Segment files that live outside the bundle body.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleExternalFiles {
    pub files: BTreeMap<PathBuf, ExternalFile>,
}

impl BundleExternalFiles {
    pub fn get(&self, path: &Path) -> Option<&ExternalFile> {
        self.files.get(path)
    }

    pub fn contains(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }

    /// External files must not shadow inline files or index-level files, and every suffix must
    /// be a plain file-name suffix so the sibling object shares the bundle's directory.
    pub fn validate(&self, inline: &BundleFileRanges) -> io::Result<()> {
        let mut suffixes = std::collections::HashSet::new();
        for (path, file) in &self.files {
            let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);
            if path == Path::new(MANAGED_JSON_PATH) || path == Path::new(META_JSON_PATH) {
                return Err(invalid(format!(
                    "index-level file {} must be stored in the footer",
                    path.display()
                )));
            }
            if inline.contains(path) {
                return Err(invalid(format!(
                    "file {} is both inline and external",
                    path.display()
                )));
            }
            if file.suffix.is_empty() || file.suffix.contains('/') || !file.suffix.starts_with('.')
            {
                return Err(invalid(format!(
                    "invalid external object suffix {:?} for {}",
                    file.suffix,
                    path.display()
                )));
            }
            if !suffixes.insert(file.suffix.as_str()) {
                return Err(invalid(format!(
                    "duplicate external object suffix {:?}",
                    file.suffix
                )));
            }
            usize::try_from(file.len).map_err(|_| {
                invalid(format!(
                    "external file {} is too large for this platform",
                    path.display()
                ))
            })?;
        }
        Ok(())
    }
}

/// Maps logical Tantivy segment paths to raw byte ranges in the bundle body.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleFileRanges {
    /// Ranges are absolute within the bundle and use `[start, end)` semantics.
    pub files: BTreeMap<PathBuf, Range<u64>>,
}

impl BundleFileRanges {
    pub fn get(&self, path: &Path) -> Option<Range<u64>> {
        self.files.get(path).cloned()
    }

    pub fn contains(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }

    /// Validates that raw files are disjoint and contained before the footer.
    pub fn validate(&self, footer_start: u64) -> io::Result<()> {
        let mut ranges = self.files.iter().collect::<Vec<_>>();
        ranges.sort_unstable_by_key(|(_, range)| (range.start, range.end));
        let mut previous_end = 0u64;
        for (path, range) in ranges {
            if path == Path::new(MANAGED_JSON_PATH) || path == Path::new(META_JSON_PATH) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "index-level file {} must be stored in the footer",
                        path.display()
                    ),
                ));
            }
            let len = range.end.checked_sub(range.start).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid reversed bundle range for {}", path.display()),
                )
            })?;
            if range.start != previous_end || range.end > footer_start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "bundle range for {} does not exactly cover the raw file region",
                        path.display()
                    ),
                ));
            }
            previous_end = range.end;
            usize::try_from(len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "bundle file {} is too large for this platform",
                        path.display()
                    ),
                )
            })?;
        }
        if previous_end != footer_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bundle file ranges do not cover the complete raw file region",
            ));
        }
        Ok(())
    }
}

/// Opaque logical bytes Tantivy needs while synchronously opening an index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleOpenSlice {
    /// Range relative to the logical segment file.
    pub range: Range<u64>,
    /// Exact bytes written by Tantivy for this range.
    #[serde(
        serialize_with = "serialize_shared_bytes",
        deserialize_with = "deserialize_shared_bytes"
    )]
    pub bytes: Arc<[u8]>,
}

fn serialize_shared_bytes<S>(bytes: &Arc<[u8]>, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    bytes.as_ref().serialize(serializer)
}

fn deserialize_shared_bytes<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
where D: serde::Deserializer<'de> {
    Vec::<u8>::deserialize(deserializer).map(Arc::from)
}

/// Complete footer payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BundleFooter {
    file_ranges: BundleFileRanges,
    external_files: BundleExternalFiles,
    open_slices: BTreeMap<PathBuf, Vec<BundleOpenSlice>>,
    managed_json: Vec<u8>,
    meta_json: Vec<u8>,
}

impl BundleFooter {
    fn file_len(&self, path: &Path) -> Option<u64> {
        if let Some(range) = self.file_ranges.get(path) {
            return Some(range.end - range.start);
        }
        self.external_files.get(path).map(|file| file.len)
    }

    fn validate(&self, footer_start: u64) -> io::Result<()> {
        self.file_ranges.validate(footer_start)?;
        self.external_files.validate(&self.file_ranges)?;
        if self.managed_json.is_empty() || self.meta_json.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer is missing an index-level file",
            ));
        }
        for (path, slices) in &self.open_slices {
            let file_len = self.file_len(path).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("open slices reference unknown file {}", path.display()),
                )
            })?;
            let mut previous_end = 0u64;
            for slice in slices {
                let slice_len =
                    slice
                        .range
                        .end
                        .checked_sub(slice.range.start)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "open slice range is reversed",
                            )
                        })?;
                if slice.range.end > file_len
                    || slice.range.start < previous_end
                    || usize::try_from(slice_len).ok() != Some(slice.bytes.len())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid open slice for {}", path.display()),
                    ));
                }
                previous_end = slice.range.end;
            }
        }
        Ok(())
    }
}

impl InvertedIndexBundleVersion {
    fn from_version_code(version: u32) -> Option<Self> {
        (version == 1).then_some(Self::V1)
    }

    fn encode_footer(footer: &BundleFooter) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        output.extend_from_slice(&BUNDLE_FOOTER_MAGIC.to_le_bytes());
        output.extend_from_slice(&(Self::V1 as u32).to_le_bytes());
        output.extend_from_slice(
            &bincode::serde::encode_to_vec(footer, bincode::config::standard())
                .map_err(io::Error::other)?,
        );
        Ok(output)
    }

    fn decode_footer(bytes: &[u8]) -> io::Result<BundleFooter> {
        const FOOTER_HEADER_LEN: usize = size_of::<u32>() * 2;
        if bytes.len() < FOOTER_HEADER_LEN {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let header = &bytes[..FOOTER_HEADER_LEN];
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != BUNDLE_FOOTER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer magic does not match",
            ));
        }
        let version_code = u32::from_le_bytes(header[4..8].try_into().unwrap());
        Self::from_version_code(version_code).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unsupported inverted-index footer version {version_code}"),
            )
        })?;

        // Bincode's limit accounts for decoded container allocations, not only encoded bytes.
        let payload = &bytes[FOOTER_HEADER_LEN..];
        let config =
            bincode::config::standard().with_limit::<{ INVERTED_INDEX_BUNDLE_DECODE_LIMIT }>();
        let (footer, consumed): (BundleFooter, usize) =
            bincode::serde::decode_from_slice(payload, config).map_err(io::Error::other)?;
        if consumed != payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer has trailing bytes",
            ));
        }
        Ok(footer)
    }
}

/// Fully validated and decoded raw Tantivy bundle footer.
///
/// The persisted object layout is:
///
/// ```text
/// [inline segment files][DBIF + footer][footer_start + version + DBIV]
/// ```
///
/// Large segment files may instead live in sibling objects `<location><suffix>`; the footer
/// records their lengths so readers can open them without another metadata request.
#[derive(Clone, Debug)]
pub struct InvertedIndexBundleFooter {
    /// Bundle wire version read from the fixed trailer.
    pub bundle_version: InvertedIndexBundleVersion,
    /// Absolute offset at which the encoded footer starts in the object.
    pub footer_start: u64,
    /// Encoded footer and fixed trailer size in bytes.
    pub footer_size: u64,
    /// Absolute object ranges of the inline raw Tantivy segment files.
    pub file_ranges: BundleFileRanges,
    /// Segment files stored as sibling objects.
    pub external_files: BundleExternalFiles,
    /// Opaque bytes required to synchronously open the Tantivy index.
    pub open_slices: BTreeMap<PathBuf, Arc<[BundleOpenSlice]>>,
    /// Original `.managed.json` bytes.
    pub managed_json: Arc<[u8]>,
    /// Original `meta.json` bytes.
    pub meta_json: Arc<[u8]>,
}

impl InvertedIndexBundleFooter {
    /// Logical length of a segment file, inline or external.
    pub fn file_len(&self, path: &Path) -> Option<u64> {
        if let Some(range) = self.file_ranges.get(path) {
            return Some(range.end - range.start);
        }
        self.external_files.get(path).map(|file| file.len)
    }

    /// Builds a bundle from inline raw segment files, references to external segment files, and
    /// footer-resident index-open data.
    pub fn build<I, P, B>(
        files: I,
        external_files: BTreeMap<PathBuf, ExternalFile>,
        open_slices: BTreeMap<PathBuf, Vec<BundleOpenSlice>>,
        managed_json: Vec<u8>,
        meta_json: Vec<u8>,
    ) -> io::Result<Vec<u8>>
    where
        I: IntoIterator<Item = (P, B)>,
        P: Into<PathBuf>,
        B: AsRef<[u8]>,
    {
        let mut output = Vec::new();
        let mut file_ranges = BTreeMap::new();
        for (path, bytes) in files {
            let path = path.into();
            if path == Path::new(MANAGED_JSON_PATH) || path == Path::new(META_JSON_PATH) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "index-level file {} must be passed separately",
                        path.display()
                    ),
                ));
            }
            if file_ranges.contains_key(&path) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate bundle path {}", path.display()),
                ));
            }
            let bytes = bytes.as_ref();
            let start = u64::try_from(output.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bundle is too large"))?;
            output.extend_from_slice(bytes);
            let end = u64::try_from(output.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bundle is too large"))?;
            file_ranges.insert(path, start..end);
        }

        let footer_start = u64::try_from(output.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bundle is too large"))?;
        let footer_and_trailer = Self::encode_footer_and_trailer(
            BundleFileRanges { files: file_ranges },
            BundleExternalFiles {
                files: external_files,
            },
            open_slices,
            managed_json,
            meta_json,
            footer_start,
        )?;
        output.extend_from_slice(&footer_and_trailer);
        Ok(output)
    }

    /// Encodes the footer and trailer that follow a raw region ending at `footer_start`.
    pub fn encode_footer_and_trailer(
        file_ranges: BundleFileRanges,
        external_files: BundleExternalFiles,
        open_slices: BTreeMap<PathBuf, Vec<BundleOpenSlice>>,
        managed_json: Vec<u8>,
        meta_json: Vec<u8>,
        footer_start: u64,
    ) -> io::Result<Vec<u8>> {
        let footer = BundleFooter {
            file_ranges,
            external_files,
            open_slices,
            managed_json,
            meta_json,
        };
        footer.validate(footer_start)?;
        let mut output = InvertedIndexBundleVersion::encode_footer(&footer)?;
        let persisted_footer_len = output
            .len()
            .checked_add(INVERTED_INDEX_BUNDLE_TRAILER_LEN)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "footer is too large"))?;
        if persisted_footer_len > INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "inverted-index footer is {persisted_footer_len} bytes; maximum is {}",
                    INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE
                ),
            ));
        }
        InvertedIndexBundleVersion::decode_footer(&output)?;
        output.extend_from_slice(&footer_start.to_le_bytes());
        output.extend_from_slice(&(InvertedIndexBundleVersion::V1 as u32).to_le_bytes());
        output.extend_from_slice(&BUNDLE_TRAILER_MAGIC);
        Ok(output)
    }

    /// Reads the persisted footer start from a complete object-tail response.
    ///
    /// Unlike `open_footer_from_tail`, this only requires the fixed trailer to be present. Readers
    /// use it after the initial 1 MiB tail read to decide whether an exact second footer read is
    /// necessary.
    pub fn footer_start_from_tail(
        tail: &[u8],
        object_size: u64,
        tail_start: u64,
    ) -> io::Result<u64> {
        let expected_len = object_size.checked_sub(tail_start).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index tail starts outside the object",
            )
        })?;
        if u64::try_from(tail.len()).ok() != Some(expected_len)
            || tail.len() < INVERTED_INDEX_BUNDLE_TRAILER_LEN
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "inverted-index tail response is incomplete",
            ));
        }
        let trailer_offset = tail.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN;
        let (footer_start, _) = Self::parse_trailer(&tail[trailer_offset..])?;
        let footer_size = object_size.checked_sub(footer_start).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer starts outside the object",
            )
        })?;
        if usize::try_from(footer_size).ok().is_none_or(|size| {
            !(INVERTED_INDEX_BUNDLE_TRAILER_LEN..=INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE)
                .contains(&size)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer size is invalid",
            ));
        }
        Ok(footer_start)
    }

    /// Extracts and validates a footer from one bounded object-tail response.
    pub fn parse_footer_from_tail(
        tail: &[u8],
        object_size: u64,
        tail_start: u64,
    ) -> io::Result<Self> {
        Self::open_footer_from_tail(tail, object_size, tail_start).map(|(_, footer)| footer)
    }

    /// Extracts and opens a footer from one complete bounded object-tail response.
    ///
    /// The returned byte slice is the persisted footer and trailer, suitable for the open cache.
    pub fn open_footer_from_tail(
        tail: &[u8],
        object_size: u64,
        tail_start: u64,
    ) -> io::Result<(&[u8], Self)> {
        let (footer_start, bundle_version, footer_bytes) =
            Self::footer_parts_from_tail(tail, object_size, tail_start)?;
        let footer = Self::decode_persisted_footer(footer_bytes, footer_start, bundle_version)?;
        Ok((footer_bytes, footer))
    }

    /// Opens persisted footer bytes and verifies that they belong to the expected object.
    pub fn open_footer_for_object(
        footer_bytes: &[u8],
        object_size: u64,
        expected_footer_start: Option<u64>,
    ) -> io::Result<Self> {
        let trailer_offset = footer_bytes
            .len()
            .checked_sub(INVERTED_INDEX_BUNDLE_TRAILER_LEN)
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        let (footer_start, bundle_version) = Self::parse_trailer(&footer_bytes[trailer_offset..])?;
        let footer_size = u64::try_from(footer_bytes.len()).map_err(io::Error::other)?;
        let cached_object_size = footer_start.checked_add(footer_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index object size overflows",
            )
        })?;
        if expected_footer_start.is_some_and(|expected| expected != footer_start)
            || cached_object_size != object_size
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer does not match object metadata",
            ));
        }
        Self::decode_persisted_footer(footer_bytes, footer_start, bundle_version)
    }

    fn parse_trailer(trailer: &[u8]) -> io::Result<(u64, InvertedIndexBundleVersion)> {
        if trailer.len() != INVERTED_INDEX_BUNDLE_TRAILER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid inverted-index bundle trailer length",
            ));
        }
        if trailer[12..16] != BUNDLE_TRAILER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index bundle magic does not match",
            ));
        }
        let footer_start = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
        let version_code = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
        let version =
            InvertedIndexBundleVersion::from_version_code(version_code).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unsupported inverted-index bundle version {version_code}"),
                )
            })?;
        Ok((footer_start, version))
    }

    fn footer_parts_from_tail(
        tail: &[u8],
        object_size: u64,
        tail_start: u64,
    ) -> io::Result<(u64, InvertedIndexBundleVersion, &[u8])> {
        let expected_len = object_size.checked_sub(tail_start).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index tail starts outside the object",
            )
        })?;
        if u64::try_from(tail.len()).ok() != Some(expected_len)
            || tail.len() < INVERTED_INDEX_BUNDLE_TRAILER_LEN
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "inverted-index tail response is incomplete",
            ));
        }

        let trailer_offset = tail.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN;
        let (footer_start, bundle_version) = Self::parse_trailer(&tail[trailer_offset..])?;
        let trailer_start = object_size
            .checked_sub(
                u64::try_from(INVERTED_INDEX_BUNDLE_TRAILER_LEN).map_err(io::Error::other)?,
            )
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "inverted-index object is shorter than its trailer",
                )
            })?;
        if footer_start < tail_start || footer_start > trailer_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer is outside the tail response",
            ));
        }
        let footer_offset = usize::try_from(footer_start - tail_start).map_err(io::Error::other)?;
        Ok((footer_start, bundle_version, &tail[footer_offset..]))
    }

    fn decode_persisted_footer(
        bytes: &[u8],
        footer_start: u64,
        bundle_version: InvertedIndexBundleVersion,
    ) -> io::Result<Self> {
        if bytes.len() < INVERTED_INDEX_BUNDLE_TRAILER_LEN {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if bytes.len() > INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer exceeds the maximum size",
            ));
        }
        let footer_end = bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN;
        let footer_bytes = &bytes[..footer_end];
        if footer_bytes.len() < 8 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let footer_version = u32::from_le_bytes(footer_bytes[4..8].try_into().unwrap());
        if footer_version != bundle_version as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer and trailer versions do not match",
            ));
        }
        let wire = InvertedIndexBundleVersion::decode_footer(footer_bytes)?;
        wire.validate(footer_start)?;
        Ok(Self {
            bundle_version,
            footer_start,
            footer_size: u64::try_from(bytes.len()).map_err(io::Error::other)?,
            file_ranges: wire.file_ranges,
            external_files: wire.external_files,
            open_slices: wire
                .open_slices
                .into_iter()
                .map(|(path, slices)| (path, Arc::from(slices)))
                .collect(),
            managed_json: Arc::from(wire.managed_json),
            meta_json: Arc::from(wire.meta_json),
        })
    }

    /// Extracts a complete footer from one bounded object-tail response.
    pub fn extract_footer_from_tail(
        tail: &[u8],
        object_size: u64,
        tail_start: u64,
    ) -> io::Result<(u64, &[u8])> {
        let (footer_start, _, footer_bytes) =
            Self::footer_parts_from_tail(tail, object_size, tail_start)?;
        Ok((footer_start, footer_bytes))
    }

    /// Opens a complete bundle already resident in memory.
    pub fn open(data: &[u8]) -> io::Result<Self> {
        if data.len() < INVERTED_INDEX_BUNDLE_TRAILER_LEN {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let trailer = &data[data.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let (footer_start, bundle_version) = Self::parse_trailer(trailer)?;
        let footer_offset = usize::try_from(footer_start).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "footer offset is too large")
        })?;
        if footer_offset > data.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index footer starts outside the object",
            ));
        }
        Self::decode_persisted_footer(&data[footer_offset..], footer_start, bundle_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_bundle() -> Vec<u8> {
        InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
            BTreeMap::new(),
            BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 0..4,
                bytes: Arc::from(b"post".as_slice()),
            }])]),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap()
    }

    fn external(suffix: &str, len: u64) -> ExternalFile {
        ExternalFile {
            suffix: suffix.to_string(),
            len,
        }
    }

    #[test]
    fn test_external_files_round_trip() {
        let bytes = InvertedIndexBundleFooter::build(
            [("segment.term", b"terms".as_slice())],
            BTreeMap::from([
                (PathBuf::from("segment.idx"), external(".idx", 4096)),
                (PathBuf::from("segment.pos"), external(".pos", 65536)),
            ]),
            BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 4090..4096,
                bytes: Arc::from(b"footer".as_slice()),
            }])]),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let footer = InvertedIndexBundleFooter::open(&bytes).unwrap();
        assert_eq!(
            footer.file_ranges.get(Path::new("segment.term")),
            Some(0..5)
        );
        assert_eq!(
            footer.external_files.get(Path::new("segment.idx")),
            Some(&external(".idx", 4096))
        );
        assert_eq!(footer.file_len(Path::new("segment.pos")), Some(65536));
        assert_eq!(footer.file_len(Path::new("segment.term")), Some(5));
        assert_eq!(footer.file_len(Path::new("segment.none")), None);
        assert_eq!(
            footer.open_slices[Path::new("segment.idx")][0].range,
            4090..4096
        );
    }

    #[test]
    fn test_bundle_location_of_sibling_objects() {
        let bundle = "1/2/_i_i_v2/gen/h0123.index";
        assert_eq!(ExternalFile::bundle_location(bundle), Some(bundle));
        assert_eq!(
            ExternalFile::bundle_location("1/2/_i_i_v2/gen/h0123.index.idx"),
            Some(bundle)
        );
        assert_eq!(
            ExternalFile::bundle_location("1/2/_i_i_v2/gen/h0123.index.pos"),
            Some(bundle)
        );
        assert_eq!(
            ExternalFile::bundle_location("1/2/_i_i_v2/gen/h0123.index/x"),
            None
        );
        assert_eq!(
            ExternalFile::bundle_location("1/2/_i_i_v2/gen/h0123.indexidx"),
            None
        );
        assert_eq!(ExternalFile::bundle_location("1/2/_b/h0123.parquet"), None);
    }

    #[test]
    fn test_external_files_are_validated() {
        let build = |external_files, open_slices| {
            InvertedIndexBundleFooter::build(
                [("segment.term", b"terms".as_slice())],
                external_files,
                open_slices,
                b"managed".to_vec(),
                b"meta".to_vec(),
            )
        };
        let ok =
            |suffix: &str| BTreeMap::from([(PathBuf::from("segment.idx"), external(suffix, 8))]);
        assert!(build(ok(".idx"), BTreeMap::new()).is_ok());
        assert!(build(ok(""), BTreeMap::new()).is_err(), "empty suffix");
        assert!(
            build(ok("idx"), BTreeMap::new()).is_err(),
            "suffix without dot"
        );
        assert!(
            build(ok("./idx"), BTreeMap::new()).is_err(),
            "suffix with separator"
        );
        let shadowing = BTreeMap::from([(PathBuf::from("segment.term"), external(".term", 5))]);
        assert!(
            build(shadowing, BTreeMap::new()).is_err(),
            "inline and external"
        );
        let duplicate_suffix = BTreeMap::from([
            (PathBuf::from("segment.idx"), external(".idx", 8)),
            (PathBuf::from("segment.pos"), external(".idx", 8)),
        ]);
        assert!(
            build(duplicate_suffix, BTreeMap::new()).is_err(),
            "duplicate suffix"
        );
        let slice_past_end =
            BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 6..10,
                bytes: Arc::from(b"abcd".as_slice()),
            }])]);
        assert!(
            build(ok(".idx"), slice_past_end).is_err(),
            "open slice beyond external len"
        );
    }

    #[test]
    fn test_open_slice_v1_wire_layout_is_unchanged() {
        #[derive(Serialize)]
        struct LegacyBundleOpenSlice {
            range: Range<u64>,
            bytes: Vec<u8>,
        }

        let legacy = LegacyBundleOpenSlice {
            range: 3..7,
            bytes: b"data".to_vec(),
        };
        let current = BundleOpenSlice {
            range: 3..7,
            bytes: Arc::from(b"data".as_slice()),
        };
        assert_eq!(
            bincode::serde::encode_to_vec(legacy, bincode::config::standard()).unwrap(),
            bincode::serde::encode_to_vec(current, bincode::config::standard()).unwrap()
        );
    }

    #[test]
    fn test_parse_footer_from_tail() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let tail_start = 3;
        let footer = InvertedIndexBundleFooter::parse_footer_from_tail(
            &bytes[tail_start as usize..],
            object_size,
            tail_start,
        )
        .unwrap();

        assert_eq!(footer.bundle_version, InvertedIndexBundleVersion::V1);
        assert_eq!(footer.footer_size, object_size - footer.footer_start);
        assert_eq!(footer.file_ranges.get(Path::new("segment.idx")), Some(0..8));
        assert_eq!(
            footer.open_slices[Path::new("segment.idx")][0]
                .bytes
                .as_ref(),
            b"post"
        );
        assert_eq!(footer.managed_json.as_ref(), b"managed");
        assert_eq!(footer.meta_json.as_ref(), b"meta");
    }

    #[test]
    fn test_footer_start_from_tail_does_not_require_complete_footer() {
        let bytes = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
            BTreeMap::new(),
            BTreeMap::new(),
            vec![b'm'; INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE + 128],
            b"meta".to_vec(),
        )
        .unwrap();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let tail_start =
            object_size - u64::try_from(INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE).unwrap();
        let tail = &bytes[usize::try_from(tail_start).unwrap()..];

        let footer_start =
            InvertedIndexBundleFooter::footer_start_from_tail(tail, object_size, tail_start)
                .unwrap();

        assert!(footer_start < tail_start);
        assert!(
            InvertedIndexBundleFooter::open_footer_from_tail(tail, object_size, tail_start)
                .is_err()
        );
        assert!(
            InvertedIndexBundleFooter::open_footer_for_object(
                &bytes[usize::try_from(footer_start).unwrap()..],
                object_size,
                Some(footer_start),
            )
            .is_ok()
        );
    }

    #[test]
    fn test_extract_footer_from_borrowed_tail() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let expected_footer_start = InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().0;
        let tail_start = expected_footer_start - 1;
        let tail = &bytes[usize::try_from(tail_start).unwrap()..];

        let (footer_start, footer_bytes) =
            InvertedIndexBundleFooter::extract_footer_from_tail(tail, object_size, tail_start)
                .unwrap();

        assert_eq!(footer_start, expected_footer_start);
        assert_eq!(
            footer_bytes,
            &bytes[usize::try_from(expected_footer_start).unwrap()..]
        );
    }

    #[test]
    fn test_open_footer_from_nonzero_tail_start() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let footer_start = InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().0;
        let tail_start = footer_start - 1;
        let tail = &bytes[usize::try_from(tail_start).unwrap()..];

        let (footer_bytes, footer) =
            InvertedIndexBundleFooter::open_footer_from_tail(tail, object_size, tail_start)
                .unwrap();

        assert_eq!(footer.bundle_version, InvertedIndexBundleVersion::V1);
        assert_eq!(footer.footer_start, footer_start);
        assert_eq!(
            footer_bytes,
            &bytes[usize::try_from(footer_start).unwrap()..]
        );
    }

    #[test]
    fn test_open_footer_from_tail_rejects_footer_outside_response() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let footer_start = InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().0;
        let tail_start = footer_start + 1;
        let tail = &bytes[usize::try_from(tail_start).unwrap()..];

        assert!(
            InvertedIndexBundleFooter::open_footer_from_tail(tail, object_size, tail_start)
                .is_err()
        );
    }

    #[test]
    fn test_open_footer_from_tail_rejects_short_response() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();

        assert!(
            InvertedIndexBundleFooter::open_footer_from_tail(
                &bytes[..bytes.len() - 1],
                object_size,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn test_open_footer_for_object_rejects_wrong_identity() {
        let bytes = build_bundle();
        let object_size = u64::try_from(bytes.len()).unwrap();
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let footer_start = InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().0;
        let footer_bytes = &bytes[usize::try_from(footer_start).unwrap()..];

        assert!(
            InvertedIndexBundleFooter::open_footer_for_object(
                footer_bytes,
                object_size + 1,
                Some(footer_start),
            )
            .is_err()
        );
        assert!(
            InvertedIndexBundleFooter::open_footer_for_object(
                footer_bytes,
                object_size,
                Some(footer_start + 1),
            )
            .is_err()
        );
    }

    #[test]
    fn test_bundle_round_trip() {
        let bytes = build_bundle();
        let footer = InvertedIndexBundleFooter::open(&bytes).unwrap();
        assert_eq!(footer.managed_json.as_ref(), b"managed");
        assert_eq!(footer.meta_json.as_ref(), b"meta");
        assert_eq!(footer.file_ranges.get(Path::new("segment.idx")), Some(0..8));
        assert!(!footer.file_ranges.contains(Path::new(MANAGED_JSON_PATH)));
        assert!(!footer.file_ranges.contains(Path::new(META_JSON_PATH)));
        assert_eq!(
            footer.open_slices[Path::new("segment.idx")][0]
                .bytes
                .as_ref(),
            b"post"
        );
    }

    #[test]
    fn test_bundle_rejects_oversized_footer() {
        let error = InvertedIndexBundleFooter::build(
            std::iter::empty::<(&str, &[u8])>(),
            BTreeMap::new(),
            BTreeMap::new(),
            vec![0; INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE],
            b"meta".to_vec(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_footer_accepts_zero_length_segment_files() {
        let footer = BundleFooter {
            external_files: BundleExternalFiles::default(),
            file_ranges: BundleFileRanges {
                files: BTreeMap::from([
                    (PathBuf::from("segment.empty"), 0..0),
                    (PathBuf::from("segment.idx"), 0..8),
                    (PathBuf::from("segment.tail"), 8..8),
                ]),
            },
            open_slices: BTreeMap::new(),
            managed_json: b"managed".to_vec(),
            meta_json: b"meta".to_vec(),
        };

        footer.validate(8).unwrap();
    }

    #[test]
    fn test_footer_rejects_non_contiguous_raw_ranges() {
        let footer = BundleFooter {
            external_files: BundleExternalFiles::default(),
            file_ranges: BundleFileRanges {
                files: BTreeMap::from([
                    (PathBuf::from("segment.idx"), 0..4),
                    (PathBuf::from("segment.pos"), 5..8),
                ]),
            },
            open_slices: BTreeMap::new(),
            managed_json: b"managed".to_vec(),
            meta_json: b"meta".to_vec(),
        };

        assert!(footer.validate(8).is_err());
    }

    #[test]
    fn test_footer_rejects_mismatched_open_slice_length() {
        let footer = BundleFooter {
            external_files: BundleExternalFiles::default(),
            file_ranges: BundleFileRanges {
                files: BTreeMap::from([(PathBuf::from("segment.idx"), 0..8)]),
            },
            open_slices: BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 0..4,
                bytes: Arc::from(b"bad".as_slice()),
            }])]),
            managed_json: b"managed".to_vec(),
            meta_json: b"meta".to_vec(),
        };

        assert!(footer.validate(8).is_err());
    }

    #[test]
    fn test_footer_and_trailer_versions_are_v1() {
        let bytes = build_bundle();
        let footer = InvertedIndexBundleFooter::open(&bytes).unwrap();
        let footer_start = usize::try_from(footer.footer_start).unwrap();
        assert_eq!(
            u32::from_le_bytes(
                bytes[footer_start + 4..footer_start + 8]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        assert_eq!(
            InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().1,
            InvertedIndexBundleVersion::V1
        );
    }

    #[test]
    fn test_bundle_rejects_unknown_trailer_version() {
        let mut bytes = build_bundle();
        let version_offset = bytes.len() - 8;
        bytes[version_offset..version_offset + 4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(
            InvertedIndexBundleFooter::open(&bytes).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test]
    fn test_bundle_rejects_unknown_footer_version() {
        let mut bytes = build_bundle();
        let trailer = &bytes[bytes.len() - INVERTED_INDEX_BUNDLE_TRAILER_LEN..];
        let footer_start =
            usize::try_from(InvertedIndexBundleFooter::parse_trailer(trailer).unwrap().0).unwrap();
        bytes[footer_start + 4..footer_start + 8].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(
            InvertedIndexBundleFooter::open(&bytes).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
