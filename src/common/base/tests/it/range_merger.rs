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

use std::fmt;
use std::fmt::Formatter;

use databend_common_base::rangemap::RangeMerger;

struct Array(Vec<std::ops::Range<u64>>);
impl fmt::Display for Array {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for range in &self.0 {
            write!(f, "[{},{}] ", range.start, range.end)?;
        }
        Ok(())
    }
}

#[test]
fn test_range_merger() -> anyhow::Result<()> {
    let v = [3..6, 1..5, 7..11, 8..9, 9..12, 4..8, 13..15, 18..20];

    let mr = RangeMerger::from_iter(v, 0, 100);
    let actual = format!("{}", Array(mr.ranges()));
    let expect = "[1,12] [13,15] [18,20] ";
    assert_eq!(actual, expect);

    Ok(())
}

#[test]
fn test_range_merger_with_gap() -> anyhow::Result<()> {
    let v = [3..6, 1..5, 7..11, 8..9, 9..12, 4..8, 13..15, 18..20];
    let not_in = [6..21, 0..0];

    // max_gap_size = 1
    {
        let mr = RangeMerger::from_iter(v.clone(), 1, 100);
        let actual = format!("{}", Array(mr.ranges()));
        let expect = "[1,15] [18,20] ";
        assert_eq!(actual, expect);

        // Check.
        {
            for check in &v {
                assert!(mr.get(check.clone()).is_some());
            }
            for ni in &not_in {
                assert!(mr.get(ni.clone()).is_none());
            }
        }
    }

    // max_gap_size = 2
    {
        let mr = RangeMerger::from_iter(v.clone(), 2, 100);
        let actual = format!("{}", Array(mr.ranges()));
        let expect = "[1,15] [18,20] ";
        assert_eq!(actual, expect);

        // Check.
        {
            for check in &v {
                assert!(mr.get(check.clone()).is_some());
            }
            for ni in &not_in {
                assert!(mr.get(ni.clone()).is_none());
            }
        }
    }

    // max_gap_size = 3
    {
        let mr = RangeMerger::from_iter(v.clone(), 3, 100);
        let actual = format!("{}", Array(mr.ranges()));
        let expect = "[1,20] ";
        assert_eq!(actual, expect);

        // Check.
        {
            for check in &v {
                assert!(mr.get(check.clone()).is_some());
            }
            for ni in &not_in {
                assert!(mr.get(ni.clone()).is_none());
            }
        }
    }

    // max_gap_size = 3, max_range_size = 5
    {
        let mr = RangeMerger::from_iter(v.clone(), 3, 4);
        let actual = format!("{}", Array(mr.ranges()));
        let expect = "[1,5] [3,8] [7,11] [8,12] [13,20] ";
        assert_eq!(actual, expect);

        // Check.
        {
            for check in v {
                assert!(mr.get(check.clone()).is_some());
            }
            for ni in &not_in {
                assert!(mr.get(ni.clone()).is_none());
            }
        }
    }

    Ok(())
}

#[test]
fn test_range_merger_with_whole_read() {
    let v = [0..10, 100..110];

    let regular = RangeMerger::from_iter(v.clone(), 0, 1000);
    assert_eq!(regular.ranges(), vec![0..10, 100..110]);

    let whole_read = RangeMerger::from_iter_with_whole_read(v, 0, 1000, 110);
    assert_eq!(whole_read.ranges(), vec![0..110]);
}
