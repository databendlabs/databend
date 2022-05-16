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

use std::result::Result;

use common_meta_types::ConflictSeq;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::SeqV;

#[test]
fn test_match_seq_match_seq_value() -> Result<(), ()> {
    assert_eq!(MatchSeq::Any.match_seq(&Some(SeqV::new(0, 1))), Ok(()));
    assert_eq!(MatchSeq::Any.match_seq(&Some(SeqV::new(1, 1))), Ok(()));

    //

    assert_eq!(
        MatchSeq::Exact(3).match_seq(&None::<SeqV>),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some(SeqV::new(0, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some(SeqV::new(2, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::Exact(3).match_seq(&Some(SeqV::new(3, 1))), Ok(()));
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some(SeqV::new(4, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::Exact(3),
            got: 4
        })
    );

    //

    assert_eq!(
        MatchSeq::GE(3).match_seq(&None::<SeqV>),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some(SeqV::new(0, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some(SeqV::new(2, 1))),
        Err(ConflictSeq::NotMatch {
            want: MatchSeq::GE(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::GE(3).match_seq(&Some(SeqV::new(3, 1))), Ok(()));
    assert_eq!(MatchSeq::GE(3).match_seq(&Some(SeqV::new(4, 1))), Ok(()));

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
    assert_eq!("is any value", MatchSeq::Any.to_string());
    assert_eq!("== 3", MatchSeq::Exact(3).to_string());
    assert_eq!(">= 3", MatchSeq::GE(3).to_string());

    Ok(())
}
