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
use std::sync::Arc;

use common_base::Runtime;
use common_dal::DataAccessor;
use common_dal::DataAccessorBuilder;
use common_exception::ErrorCode;
use common_meta_types::NodeInfo;

/// Methods for a table to get resource handles it needs to read/write.
///
/// Common usages:
/// - A table needs to gather block statistics to build a `plan`.
/// - A table needs to append a block to the underlying storage to complete an **insert** operation.
pub trait IOContext {
    /// Get a runtime that schedules async tasks.
    // fn get_runtime(&self) -> Arc<dyn TrySpawn>;
    fn get_runtime(&self) -> Arc<Runtime>;

    fn get_data_accessor(&self) -> Result<Arc<dyn DataAccessor>, ErrorCode>;

    fn get_max_threads(&self) -> usize;

    /// Get a vec of `query` nodes.
    fn get_query_nodes(&self) -> Vec<Arc<NodeInfo>>;

    /// Get a vec of `query` node ids.
    fn get_query_node_ids(&self) -> Vec<String>;

    /// Get user defined context data
    fn get_user_data_any(&self) -> Option<Arc<dyn Any + Send + Sync + 'static>>;

    /// Get user defined data as type T
    fn get_user_data<T: Debug + Send + Sync + 'static>(&self) -> Result<Option<Arc<T>>, ErrorCode> {
        let ud = self.get_user_data_any();
        match ud {
            None => Ok(None),
            Some(x) => {
                let v = x.downcast::<T>();
                match v {
                    Ok(x) => Ok(Some(x)),
                    Err(_) => {
                        let e = ErrorCode::InvalidCast("invalid cast");
                        Err(e)
                    }
                }
            }
        }
    }
}

pub struct TableIOContext {
    runtime: Arc<Runtime>,
    data_accessor_builder: Arc<dyn DataAccessorBuilder>,
    max_threads: usize,
    query_nodes: Vec<Arc<NodeInfo>>,
    user_data: Option<Arc<dyn Any + Send + Sync + 'static>>,
}

impl TableIOContext {
    pub fn new(
        rt: Arc<Runtime>,
        dab: Arc<dyn DataAccessorBuilder>,
        max_threads: usize,
        query_nodes: Vec<Arc<NodeInfo>>,
        user_data: Option<Arc<dyn Any + Send + Sync + 'static>>,
    ) -> TableIOContext {
        TableIOContext {
            runtime: rt,
            data_accessor_builder: dab,
            max_threads,
            query_nodes,
            user_data,
        }
    }
}

impl IOContext for TableIOContext {
    fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    fn get_data_accessor(&self) -> Result<Arc<dyn DataAccessor>, ErrorCode> {
        self.data_accessor_builder.build()
    }

    fn get_max_threads(&self) -> usize {
        self.max_threads
    }

    fn get_query_nodes(&self) -> Vec<Arc<NodeInfo>> {
        self.query_nodes.clone()
    }

    fn get_query_node_ids(&self) -> Vec<String> {
        self.query_nodes.iter().map(|x| x.id.clone()).collect()
    }

    fn get_user_data_any(&self) -> Option<Arc<dyn Any + Send + Sync + 'static>> {
        self.user_data.clone()
    }
}
