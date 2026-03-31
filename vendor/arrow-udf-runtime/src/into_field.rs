// Copyright 2024 RisingWave Labs
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

use arrow_schema::{DataType, Field};

/// Converts a type into a [`Field`].
/// Implementors are [`DataType`] and [`Field`].
pub trait IntoField: private::Sealed {
    fn into_field(self, default_name: &str) -> Field;
}

impl IntoField for Field {
    fn into_field(self, _default_name: &str) -> Field {
        self
    }
}

impl IntoField for DataType {
    fn into_field(self, default_name: &str) -> Field {
        Field::new(default_name, self, true)
    }
}

mod private {
    use arrow_schema::{DataType, Field};

    pub trait Sealed {}
    impl Sealed for Field {}
    impl Sealed for DataType {}
}
