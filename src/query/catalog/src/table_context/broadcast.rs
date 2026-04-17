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

pub trait TableContextBroadcast: Send + Sync {
    /// Calling this function will automatically create a pipeline for broadcast data in
    /// `build_distributed_pipeline()`.
    ///
    /// The returned id can be used to get sender and receiver for broadcasting data.
    fn get_next_broadcast_id(&self) -> u32;

    fn reset_broadcast_id(&self) {
        unimplemented!()
    }
}
