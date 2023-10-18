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

use crate::error::{Error, Result};

pub struct StageLocation {
    pub name: String,
    pub path: String,
}

impl std::fmt::Display for StageLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}/{}", self.name, self.path)
    }
}

impl TryFrom<&str> for StageLocation {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        if !s.starts_with('@') {
            return Err(Error::Parsing(format!("Invalid stage location: {}", s)));
        }
        let mut parts = s.splitn(2, '/');
        let name = parts
            .next()
            .ok_or_else(|| Error::Parsing(format!("Invalid stage location: {}", s)))?
            .trim_start_matches('@');
        let path = parts.next().unwrap_or_default();
        Ok(Self {
            name: name.to_string(),
            path: path.to_string(),
        })
    }
}

impl StageLocation {
    pub fn file_path(&self, file_name: &str) -> String {
        if self.path.ends_with('/') {
            format!("{}{}", self, file_name)
        } else {
            format!("{}/{}", self, file_name)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_stage() -> Result<()> {
        let location = "@stage_name/path/to/file";
        let stage = StageLocation::try_from(location)?;
        assert_eq!(stage.name, "stage_name");
        assert_eq!(stage.path, "path/to/file");
        Ok(())
    }

    #[test]
    fn parse_stage_empty_path() -> Result<()> {
        let location = "@stage_name";
        let stage = StageLocation::try_from(location)?;
        assert_eq!(stage.name, "stage_name");
        assert_eq!(stage.path, "");
        Ok(())
    }

    #[test]
    fn parse_stage_fail() -> Result<()> {
        let location = "stage_name/path/to/file";
        let stage = StageLocation::try_from(location);
        assert!(stage.is_err());
        Ok(())
    }
}
