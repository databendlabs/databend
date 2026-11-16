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

//! The retrievers of a search, a file each with its spec.

pub mod text;
pub mod vector;

use std::sync::Arc;

use databend_common_exception::Result;

use crate::data::core::TableTarget;
use crate::data::search::Retriever;
use crate::data::search::RetrieverSpec;

/// The retriever of `spec` for `target`, finding the `candidates` best rows of a block.
pub async fn build_retriever(
    target: &TableTarget,
    spec: &RetrieverSpec,
    candidates: usize,
) -> Result<Arc<dyn Retriever>> {
    match spec {
        RetrieverSpec::Text(spec) => Ok(Arc::new(
            text::TextRetriever::create(target, spec, candidates).await?,
        )),
        RetrieverSpec::Vector(spec) => Ok(Arc::new(
            vector::VectorRetriever::create(target, spec, candidates).await?,
        )),
    }
}
