// Copyright 2020 Datafuse Labs.
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

use std::result::Result;

use crate::ConflictSeq;
use crate::MatchSeq;
use crate::MatchSeqExt;
use crate::SeqValue;

#[test]
fn test_match_seq_match_seq_value() -> Result<(), ()> {
    assert_eq!(MatchSeq::Any.match_seq(&Some((0, 1))), Ok(()));
    assert_eq!(MatchSeq::Any.match_seq(&Some((1, 1))), Ok(()));

    //

    assert_eq!(
        MatchSeq::Exact(3).match_seq(&None::<SeqValue>),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((0, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((2, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::Exact(3).match_seq(&Some((3, 1))), Ok(()));
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((4, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 4
        })
    );

    //

    assert_eq!(
        MatchSeq::GE(3).match_seq(&None::<SeqValue>),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some((0, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some((2, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::GE(3).match_seq(&Some((3, 1))), Ok(()));
    assert_eq!(MatchSeq::GE(3).match_seq(&Some((4, 1))), Ok(()));

    Ok(())
}

#[test]
fn test_match_seq_from_opt_u64() -> Result<(), ()> {
    assert_eq!(MatchSeq::Exact(3), Some(3).into());
    assert_eq!(MatchSeq::Any, None.into());

    Ok(())
}

#[test]
fn test_match_seq_display() -> Result<(), ()> {
    assert_eq!("is any value", format!("{}", MatchSeq::Any));
    assert_eq!("== 3", format!("{}", MatchSeq::Exact(3)));
    assert_eq!(">= 3", format!("{}", MatchSeq::GE(3)));

    Ok(())
}
