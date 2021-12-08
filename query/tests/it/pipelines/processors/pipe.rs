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

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use databend_query::pipelines::processors::*;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipe() -> Result<()> {
    let mut pipe = Pipe::create();
    let empty = Arc::new(EmptyProcessor::create());
    pipe.add(empty.clone());

    let num = pipe.nums();
    assert_eq!(1, num);

    let get_empty = pipe.processor_by_index(0);
    assert_eq!(empty.name(), get_empty.name());

    let first = pipe.first();
    assert_eq!(empty.name(), first.name());

    let processors = pipe.processors();
    assert_eq!(empty.name(), processors[0].name());

    Ok(())
}
