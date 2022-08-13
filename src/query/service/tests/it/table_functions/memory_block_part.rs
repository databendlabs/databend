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

use common_exception::Result;
use databend_query::table_functions::generate_numbers_parts;
use databend_query::table_functions::NumbersPartInfo;
use pretty_assertions::assert_eq;

#[test]
fn test_util_generate_parts() -> Result<()> {
    {
        // deal with remainder
        let ps = generate_numbers_parts(0, 3, 11);

        assert_eq!(3, ps.len());

        let numbers_part = NumbersPartInfo::from_part(&ps[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 3);
        assert_eq!(numbers_part.total, 11);

        let numbers_part = NumbersPartInfo::from_part(&ps[1])?;
        assert_eq!(numbers_part.part_start, 3);
        assert_eq!(numbers_part.part_end, 6);
        assert_eq!(numbers_part.total, 11);

        let numbers_part = NumbersPartInfo::from_part(&ps[2])?;
        assert_eq!(numbers_part.part_start, 6);
        assert_eq!(numbers_part.part_end, 11);
        assert_eq!(numbers_part.total, 11);
    }

    {
        // total is zero
        let ps = generate_numbers_parts(0, 3, 0);

        assert_eq!(1, ps.len());
        let numbers_part = NumbersPartInfo::from_part(&ps[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 0);
        assert_eq!(numbers_part.total, 0);
    }
    {
        // only one part, total < workers
        let ps = generate_numbers_parts(0, 3, 2);

        assert_eq!(1, ps.len());
        let numbers_part = NumbersPartInfo::from_part(&ps[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 2);
        assert_eq!(numbers_part.total, 2);
    }

    Ok(())
}
