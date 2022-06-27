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

use common_exception::Result;
use databend_query::formats::output_format::OutputFormatType;
use databend_query::formats::FormatFactory;
use strum::IntoEnumIterator;

#[test]
fn test_outputs() -> Result<()> {
    let factory = FormatFactory::instance();
    for t in OutputFormatType::iter() {
        let name = t.to_string();
        assert_eq!(factory.get_output(&name)?.to_string(), name);
    }
    assert_eq!(factory.get_output("NDJson")?, OutputFormatType::JsonEachRow);
    assert_eq!(
        factory.get_output("TabSeparatedWithNames")?,
        OutputFormatType::TSVWithNames
    );
    assert_eq!(
        factory.get_output("TabSeparatedWithNamesAndTypes")?,
        OutputFormatType::TSVWithNamesAndTypes
    );
    Ok(())
}
