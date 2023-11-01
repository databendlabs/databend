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

#[derive(Debug)]
pub struct SelectVector {
    increment: bool,
    sel_vector: Vec<usize>,
}

impl Default for SelectVector {
    fn default() -> Self {
        Self {
            increment: true,
            sel_vector: vec![],
        }
    }
}

impl SelectVector {
    pub fn auto_increment() -> Self {
        Self::default()
    }

    pub fn new(size: usize) -> Self {
        Self {
            increment: false,
            sel_vector: vec![0; size],
        }
    }

    pub fn resize(&mut self, new_len: usize) {
        self.increment = false;
        self.sel_vector.resize(new_len, 0);
    }

    pub fn with_sel(&mut self, sel_vection: Vec<usize>) {
        self.sel_vector = sel_vection;
    }

    // these function did not check index boundes
    // keep in mind when using them
    pub fn set_index(&mut self, idx: usize, loc: usize) {
        self.sel_vector[idx] = loc;
    }

    pub fn get_index(&self, idx: usize) -> usize {
        if self.increment {
            idx
        } else {
            self.sel_vector[idx]
        }
    }

    pub fn swap(&mut self, i: usize, j: usize) {
        self.sel_vector.swap(i, j);
    }
}
