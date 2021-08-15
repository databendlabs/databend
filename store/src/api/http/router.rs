// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use warp::Filter;

use crate::configs::Config;

pub struct Router {
    cfg: Config,
}

impl Router {
    pub fn create(cfg: Config) -> Self {
        Router { cfg }
    }

    pub fn router(
        &self,
    ) -> Result<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone> {
        let v1 = super::v1::config::config_handler(self.cfg.clone())
            .or(super::debug::home::debug_handler(self.cfg.clone()));
        let routes = v1.with(warp::log("v1"));
        Ok(routes)
    }
}
