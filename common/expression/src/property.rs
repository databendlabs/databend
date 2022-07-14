// Copyright 2022 Datafuse Labs.
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

// pub type ValueRange<T: ValueType> = (Option<T::Scalar>, Option<T::Scalar>);

#[derive(Debug, Clone, Default)]
pub struct ValueProperty {
    pub not_null: bool,
    // pub range: ValueRange<AnyType>,
}

#[derive(Clone, Default)]
pub struct FunctionProperty {
    pub preserve_not_null: bool,
    pub commutative: bool,
}

impl ValueProperty {
    pub fn not_null(mut self, not_null: bool) -> Self {
        self.not_null = not_null;
        self
    }
}

impl FunctionProperty {
    pub fn preserve_not_null(mut self, preserve_not_null: bool) -> Self {
        self.preserve_not_null = preserve_not_null;
        self
    }

    pub fn commutative(mut self, commutative: bool) -> Self {
        self.commutative = commutative;
        self
    }
}

// impl FunctionProperty<AnyType> {
//     pub fn apply(&self, args: &[ValueProperty]) -> ValueProperty {
//         let not_null = self.preserve_not_null && args.iter().all(|arg| arg.not_null);
//         let range = self
//             .domain_to_range
//             .as_ref()
//             .and_then(|f| {
//                 f(args
//                     .iter()
//                     .map(|arg| arg.range.clone())
//                     .collect::<Vec<_>>()
//                     .as_slice())
//             })
//             .unwrap_or((None, None));
//         ValueProperty { not_null, range }
//     }
// }
