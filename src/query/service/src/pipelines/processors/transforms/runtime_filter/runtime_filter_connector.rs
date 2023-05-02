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

use common_exception::Result;
use common_expression::DataBlock;

// The trait will connect runtime filter source of join build side
// with join probe side
#[async_trait::async_trait]
pub trait RuntimeFilterConnector: Send + Sync {
    fn attach(&self);

    fn detach(&self) -> Result<()>;

    fn is_finished(&self) -> Result<bool>;

    async fn wait_finish(&self) -> Result<()>;

    // Consume runtime filter for blocks from join probe side
    fn consume(&self, data: &DataBlock) -> Result<Vec<DataBlock>>;

    // Collect runtime filter from join build side
    fn collect(&self, data: &DataBlock) -> Result<()>;
}
