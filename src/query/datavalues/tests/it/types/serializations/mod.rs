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

use common_datavalues::prelude::*;
use common_datavalues::serializations::NullSerializer;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_convert_arrow() {
    let t = TimestampType::new_impl();
    let arrow_y = t.to_arrow_field("x");
    let new_t = from_arrow_field(&arrow_y);

    assert_eq!(new_t.name(), t.name())
}

#[test]
fn test_enum_dispatch() -> Result<()> {
    let c = NullSerializer { size: 0 };
    let d: TypeSerializerImpl = c.into();
    let _: NullSerializer = d.try_into()?;
    Ok(())
}
