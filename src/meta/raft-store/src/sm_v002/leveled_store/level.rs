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

use std::fmt;
use std::sync::Arc;

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::leveled_map::MultiLevelMap;
use crate::sm_v002::leveled_store::map_api::MapApi;

/// One level of state machine data.
///
/// State machine data is constructed from multiple levels of modifications, similar to leveldb.
#[derive(Debug, Default)]
pub struct Level {
    data: LevelData,

    /// This level is built with additional modifications `data` on top of the previous level.
    base: Option<Arc<Level>>,
}

impl<K> MultiLevelMap<K> for Level
where
    K: Ord + fmt::Debug,
    LevelData: MapApi<K>,
{
    type API = LevelData;

    fn data<'a>(&'a self) -> &Self::API
    where Self::API: 'a {
        &self.data
    }

    fn data_mut<'a>(&'a mut self) -> &mut Self::API
    where Self::API: 'a {
        &mut self.data
    }

    fn base(&self) -> Option<&Self> {
        self.base.as_ref().map(|x| x.as_ref())
    }
}

impl Level {
    pub fn new(data: LevelData, base: Option<Arc<Self>>) -> Self {
        Self { data, base }
    }

    pub fn new_level(&mut self) {
        let new = Level {
            data: self.data.new_level(),
            base: None,
        };

        let base = std::mem::replace(self, new);

        self.base = Some(Arc::new(base));
    }

    pub fn data_ref(&self) -> &LevelData {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut LevelData {
        &mut self.data
    }

    pub fn get_base(&self) -> Option<Arc<Self>> {
        self.base.clone()
    }

    pub(crate) fn replace_base(&mut self, b: Option<Arc<Level>>) {
        self.base = b;
    }

    pub fn snapshot(&self) -> Option<Arc<Self>> {
        self.base.clone()
    }
}
