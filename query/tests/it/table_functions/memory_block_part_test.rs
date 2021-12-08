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
use common_planners::Part;
use databend_query::table_functions::generate_block_parts;
use pretty_assertions::assert_eq;

#[test]
fn test_util_generate_parts() -> Result<()> {
    {
        // deal with remainder
        let ps = generate_block_parts(0, 3, 11);

        assert_eq!(3, ps.len());
        assert_eq!(
            Part {
                name: "11-0-3".into(),
                version: 0
            },
            ps[0]
        );
        assert_eq!(
            Part {
                name: "11-3-6".into(),
                version: 0
            },
            ps[1]
        );
        assert_eq!(
            Part {
                name: "11-6-11".into(),
                version: 0
            },
            ps[2]
        );
    }

    {
        // total is zero
        let ps = generate_block_parts(0, 3, 0);

        assert_eq!(1, ps.len());
        assert_eq!(
            Part {
                name: "0-0-0".into(),
                version: 0
            },
            ps[0]
        );
    }
    {
        // only one part, total < workers
        let ps = generate_block_parts(0, 3, 2);

        assert_eq!(1, ps.len());
        assert_eq!(
            Part {
                name: "2-0-2".into(),
                version: 0
            },
            ps[0]
        );
    }

    Ok(())
}
