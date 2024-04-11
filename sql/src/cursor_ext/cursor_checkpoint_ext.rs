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

use std::io::Cursor;

pub trait ReadCheckPointExt {
    fn checkpoint(&self) -> u64;
    fn rollback(&mut self, checkpoint: u64);
}

impl<T> ReadCheckPointExt for Cursor<T>
where
    T: AsRef<[u8]>,
{
    fn checkpoint(&self) -> u64 {
        self.position()
    }

    fn rollback(&mut self, checkpoint: u64) {
        self.set_position(checkpoint)
    }
}
