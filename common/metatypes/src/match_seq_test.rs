// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::MatchSeq;
use crate::MatchSeqExt;
use crate::SeqError;
use crate::SeqValue;

#[test]
fn test_seq_error_display() -> anyhow::Result<()> {
    assert_eq!(
        "seq not match, expect: 1 is any value",
        SeqError::NotMatch {
            want: MatchSeq::Any,
            got: 1
        }
        .to_string()
    );

    assert_eq!(
        "seq not match, expect: 1 == 5",
        SeqError::NotMatch {
            want: MatchSeq::Exact(5),
            got: 1
        }
        .to_string()
    );

    assert_eq!(
        "seq not match, expect: 1 >= 5",
        SeqError::NotMatch {
            want: MatchSeq::GE(5),
            got: 1
        }
        .to_string()
    );

    Ok(())
}

#[test]
fn test_match_seq_match_seq_value() -> anyhow::Result<()> {
    assert_eq!(MatchSeq::Any.match_seq(&Some((0, 1))), Ok(()));
    assert_eq!(MatchSeq::Any.match_seq(&Some((1, 1))), Ok(()));

    //

    assert_eq!(
        MatchSeq::Exact(3).match_seq(&None::<SeqValue>),
        Err(SeqError::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((0, 1))),
        Err(SeqError::NotMatch {
            want: MatchSeq::Exact(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((2, 1))),
        Err(SeqError::NotMatch {
            want: MatchSeq::Exact(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::Exact(3).match_seq(&Some((3, 1))), Ok(()));
    assert_eq!(
        MatchSeq::Exact(3).match_seq(&Some((4, 1))),
        Err(SeqError::NotMatch {
            want: MatchSeq::Exact(3),
            got: 4
        })
    );

    //

    assert_eq!(
        MatchSeq::GE(3).match_seq(&None::<SeqValue>),
        Err(SeqError::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some((0, 1))),
        Err(SeqError::NotMatch {
            want: MatchSeq::GE(3),
            got: 0
        })
    );
    assert_eq!(
        MatchSeq::GE(3).match_seq(&Some((2, 1))),
        Err(SeqError::NotMatch {
            want: MatchSeq::GE(3),
            got: 2
        })
    );
    assert_eq!(MatchSeq::GE(3).match_seq(&Some((3, 1))), Ok(()));
    assert_eq!(MatchSeq::GE(3).match_seq(&Some((4, 1))), Ok(()));

    Ok(())
}

#[test]
fn test_match_seq_from_opt_u64() -> anyhow::Result<()> {
    assert_eq!(MatchSeq::Exact(3), Some(3).into());
    assert_eq!(MatchSeq::Any, None.into());

    Ok(())
}

#[test]
fn test_match_seq_display() -> anyhow::Result<()> {
    assert_eq!("is any value", format!("{}", MatchSeq::Any));
    assert_eq!("== 3", format!("{}", MatchSeq::Exact(3)));
    assert_eq!(">= 3", format!("{}", MatchSeq::GE(3)));

    Ok(())
}
