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

use std::fmt;

// TODO Using strings as a keys
#[derive(Clone, Copy, Default)]
pub struct Enum8(pub(crate) i8);

#[derive(Clone, Copy, Default)]
pub struct Enum16(pub(crate) i16);

impl PartialEq for Enum16 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for Enum16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl fmt::Debug for Enum16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl Enum16 {
    pub fn of(source: i16) -> Self {
        Self(source)
    }
    #[inline(always)]
    pub fn internal(self) -> i16 {
        self.0
    }
}
impl PartialEq for Enum8 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for Enum8 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum8({})", self.0)
    }
}

impl fmt::Debug for Enum8 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum8({})", self.0)
    }
}

impl Enum8 {
    pub fn of(source: i8) -> Self {
        Self(source)
    }
    #[inline(always)]
    pub fn internal(self) -> i8 {
        self.0
    }
}
