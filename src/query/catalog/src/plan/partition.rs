// Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

#[typetag::serde(tag = "type")]
pub trait PartInfo: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    #[allow(clippy::borrowed_box)]
    fn equals(&self, info: &Box<dyn PartInfo>) -> bool;
}

impl Debug for Box<dyn PartInfo> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(str) => write!(f, "{}", str),
            Err(_cause) => Err(std::fmt::Error {}),
        }
    }
}

impl PartialEq for Box<dyn PartInfo> {
    fn eq(&self, other: &Self) -> bool {
        let this_type_id = self.as_any().type_id();
        let other_type_id = other.as_any().type_id();

        match this_type_id == other_type_id {
            true => self.equals(other),
            false => false,
        }
    }
}

#[allow(dead_code)]
pub type PartInfoPtr = Arc<Box<dyn PartInfo>>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub enum PartitionsShuffleKind {
    None,
    Mod,
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Partitions {
    pub kind: PartitionsShuffleKind,
    pub partitions: Vec<PartInfoPtr>,
}

impl Partitions {
    pub fn create(kind: PartitionsShuffleKind, partitions: Vec<PartInfoPtr>) -> Self {
        Partitions { kind, partitions }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            kind: PartitionsShuffleKind::None,
            partitions: vec![],
        }
    }
}
