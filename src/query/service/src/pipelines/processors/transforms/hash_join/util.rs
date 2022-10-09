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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;

pub(crate) fn build_schema_wrap_nullable(build_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(build_schema.fields().len());
    for field in build_schema.fields() {
        nullable_field.push(DataField::new_nullable(
            field.name(),
            field.data_type().clone(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}

pub(crate) fn probe_schema_wrap_nullable(probe_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(probe_schema.fields().len());
    for field in probe_schema.fields() {
        nullable_field.push(DataField::new_nullable(
            field.name(),
            field.data_type().clone(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}
