// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Display;
use std::str::FromStr;

use uuid::Uuid;

use crate::{Error, ErrorKind, Result};

/// Helper for parsing a location of the format: `<location>/metadata/<version>-<uuid>.metadata.json`
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    table_location: String,
    version: i32,
    id: Uuid,
}

impl MetadataLocation {
    /// Creates a completely new metadata location starting at version 0.
    /// Only used for creating a new table. For updates, see `with_next_version`.
    pub fn new_with_table_location(table_location: impl ToString) -> Self {
        Self {
            table_location: table_location.to_string(),
            version: 0,
            id: Uuid::new_v4(),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    pub fn with_next_version(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
        }
    }

    fn parse_metadata_path_prefix(path: &str) -> Result<String> {
        let prefix = path.strip_suffix("/metadata").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Metadata location not under \"/metadata\" subdirectory: {path}"),
        ))?;

        Ok(prefix.to_string())
    }

    /// Parses a file name of the format `<version>-<uuid>.metadata.json`.
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid)> {
        let (version, id) = file_name
            .strip_suffix(".metadata.json")
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Invalid metadata file ending: {file_name}"),
            ))?
            .split_once('-')
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Invalid metadata file name format: {file_name}"),
            ))?;

        Ok((version.parse::<i32>()?, Uuid::parse_str(id)?))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/metadata/{:0>5}-{}.metadata.json",
            self.table_location, self.version, self.id
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (path, file_name) = s.rsplit_once('/').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata location: {s}"),
        ))?;

        let prefix = Self::parse_metadata_path_prefix(path)?;
        let (version, id) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            table_location: prefix,
            version,
            id,
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use uuid::Uuid;

    use crate::MetadataLocation;

    #[test]
    fn test_metadata_location_from_string() {
        let test_cases = vec![
            // No prefix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                }),
            ),
            // Some prefix
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                }),
            ),
            // Longer prefix
            (
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc/def".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                }),
            ),
            // Prefix with special characters
            (
                "https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "https://127.0.0.1".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                }),
            ),
            // Another id
            (
                "/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("81056704-ce5b-41c4-bb83-eb6408081af6").unwrap(),
                }),
            ),
            // Version 0
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 0,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                }),
            ),
            // Negative version
            (
                "/metadata/-123-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // Invalid uuid
            (
                "/metadata/1234567-no-valid-id.metadata.json",
                Err("".to_string()),
            ),
            // Non-numeric version
            (
                "/metadata/noversion-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // No /metadata subdirectory
            (
                "/wrongsubdir/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // No .metadata.json suffix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata",
                Err("".to_string()),
            ),
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.wrong.file",
                Err("".to_string()),
            ),
        ];

        for (input, expected) in test_cases {
            match MetadataLocation::from_str(input) {
                Ok(metadata_location) => {
                    assert!(expected.is_ok());
                    assert_eq!(metadata_location, expected.unwrap());
                }
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

    #[test]
    fn test_metadata_location_with_next_version() {
        let test_cases = vec![
            MetadataLocation::new_with_table_location("/abc"),
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = MetadataLocation::from_str(&input.to_string())
                .unwrap()
                .with_next_version();
            assert_eq!(next.table_location, input.table_location);
            assert_eq!(next.version, input.version + 1);
            assert_ne!(next.id, input.id);
        }
    }
}
