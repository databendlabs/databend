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

use databend_common_expression::types::DataType;

pub trait Index {
    fn supported_type(data_type: &DataType) -> bool {
        // we support nullable column but Nulls are not added into the bloom filter.
        let inner_type = data_type.remove_nullable();
        matches!(
            inner_type,
            DataType::Number(_)
                | DataType::Date
                | DataType::Timestamp
                | DataType::String
                | DataType::Decimal(_)
        )
    }
}
