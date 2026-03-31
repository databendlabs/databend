// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub mod datatypes;
pub mod format;
pub(crate) mod io;
pub mod previous;
pub mod reader;
pub mod testing;
pub mod writer;

pub use io::LanceEncodingsIo;

use format::MAGIC;
pub use lance_encoding::version;

use lance_core::{Error, Result};
use lance_encoding::version::LanceFileVersion;
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use snafu::location;

pub async fn determine_file_version(
    store: &ObjectStore,
    path: &Path,
    known_size: Option<usize>,
) -> Result<LanceFileVersion> {
    let size = match known_size {
        None => store.size(path).await.unwrap() as usize,
        Some(size) => size,
    };
    if size < 8 {
        return Err(Error::InvalidInput {
            source: format!(
                "the file {} does not appear to be a lance file (too small)",
                path
            )
            .into(),
            location: location!(),
        });
    }
    let reader = store.open_with_size(path, size).await?;
    let footer = reader.get_range((size - 8)..size).await?;
    if &footer[4..] != MAGIC {
        return Err(Error::InvalidInput {
            source: format!(
                "the file {} does not appear to be a lance file (magic mismatch)",
                path
            )
            .into(),
            location: location!(),
        });
    }
    let major_version = u16::from_le_bytes([footer[0], footer[1]]);
    let minor_version = u16::from_le_bytes([footer[2], footer[3]]);

    LanceFileVersion::try_from_major_minor(major_version as u32, minor_version as u32)
}
