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

//! Resolve local civil times through timezone folds and gaps.

use chrono::LocalResult;
use chrono::NaiveDateTime;
use chrono::Offset;
use chrono::TimeZone as _;
use chrono_tz::GapInfo;
use chrono_tz::Tz;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedLocalTime {
    pub unix_seconds: i64,
    pub offset_seconds: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalTimeResolution {
    /// Earlier fold candidate; move gaps forward by their actual width.
    #[default]
    Compatible,
    Earlier,
    Later,
    Reject,
}

/// Resolve a local time according to `resolution`.
/// `preferred_offset` selects that side of a fold when available.
pub fn resolve_local_datetime(
    tz: &Tz,
    local: NaiveDateTime,
    resolution: LocalTimeResolution,
    preferred_offset: Option<i32>,
) -> Option<ResolvedLocalTime> {
    resolve_named(tz, local, resolution, preferred_offset)
}

fn resolve_named(
    tz: &Tz,
    local: NaiveDateTime,
    resolution: LocalTimeResolution,
    preferred_offset: Option<i32>,
) -> Option<ResolvedLocalTime> {
    match tz.from_local_datetime(&local) {
        LocalResult::Single(value) => Some(resolved(value.timestamp(), &value)),

        // `chrono` documents this pair as (earliest, latest).
        LocalResult::Ambiguous(earlier, later) => {
            if let Some(preferred) = preferred_offset {
                if earlier.offset().fix().local_minus_utc() == preferred {
                    return Some(resolved(earlier.timestamp(), &earlier));
                }
                if later.offset().fix().local_minus_utc() == preferred {
                    return Some(resolved(later.timestamp(), &later));
                }
            }

            match resolution {
                LocalTimeResolution::Compatible | LocalTimeResolution::Earlier => {
                    Some(resolved(earlier.timestamp(), &earlier))
                }
                LocalTimeResolution::Later => Some(resolved(later.timestamp(), &later)),
                LocalTimeResolution::Reject => None,
            }
        }

        // `LocalResult::None` also covers arithmetic overflow, so a missing
        // `GapInfo` is treated as unresolvable rather than assumed to be a gap.
        LocalResult::None => {
            if matches!(resolution, LocalTimeResolution::Reject) {
                return None;
            }

            let gap = GapInfo::new(&local, tz)?;
            let (gap_begin, _) = gap.begin?;
            let gap_end = gap.end?;
            let gap_width = gap_end.naive_local().signed_duration_since(gap_begin);

            let adjusted = match resolution {
                LocalTimeResolution::Earlier => local.checked_sub_signed(gap_width)?,
                _ => local.checked_add_signed(gap_width)?,
            };

            let resolved_value = tz.from_local_datetime(&adjusted).single()?;
            Some(resolved(resolved_value.timestamp(), &resolved_value))
        }
    }
}

fn resolved<Tz: chrono::TimeZone>(
    unix_seconds: i64,
    value: &chrono::DateTime<Tz>,
) -> ResolvedLocalTime {
    ResolvedLocalTime {
        unix_seconds,
        offset_seconds: value.offset().fix().local_minus_utc(),
    }
}
