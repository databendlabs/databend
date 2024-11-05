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

use gimli::Attribute;
use gimli::AttributeValue;
use gimli::EntriesTreeNode;
use gimli::RangeListsOffset;
use gimli::Reader;
use gimli::UnitOffset;

use crate::elf::dwarf_unit::Unit;
use crate::exception_backtrace_elf::HighPc;

pub struct SubprogramAttrs<R: Reader> {
    high_pc: Option<HighPc>,
    low_pc: Option<u64>,
    ranges_offset: Option<RangeListsOffset<R::Offset>>,
}

impl<R: Reader> SubprogramAttrs<R> {
    pub fn create() -> SubprogramAttrs<R> {
        SubprogramAttrs {
            high_pc: None,
            low_pc: None,
            ranges_offset: None,
        }
    }

    pub fn set_attr(&mut self, attr: Attribute<R>) {
        match (attr.name(), attr.value()) {
            (gimli::DW_AT_high_pc, AttributeValue::Addr(addr)) => {
                self.high_pc = Some(HighPc::Addr(addr));
            }
            (gimli::DW_AT_high_pc, AttributeValue::Udata(offset)) => {
                self.high_pc = Some(HighPc::Offset(offset));
            }
            (gimli::DW_AT_low_pc, AttributeValue::Addr(v)) => {
                self.low_pc = Some(v);
            }
            (gimli::DW_AT_ranges, AttributeValue::RangeListsRef(v)) => {
                self.ranges_offset = Some(RangeListsOffset(v.0));
            }
            _ => {}
        }
    }

    pub fn match_pc(&self, probe: u64) -> bool {
        match (self.low_pc, self.high_pc) {
            (Some(low), Some(high)) => {
                probe >= low
                    && match high {
                    HighPc::Addr(high) => probe < high,
                    HighPc::Offset(size) => probe < low + size,
                }
            }
            _ => false,
        }
    }
}

impl<R: Reader> Unit<R> {
    pub(crate) fn find_subprogram(&self, probe: u64) -> gimli::Result<Option<UnitOffset<R::Offset>>> {
        let entries = self.head.entries_raw(&self.abbreviations, None)?;

        while !entries.is_empty() {
            let dw_die_offset = entries.next_offset();
            if let Some(abbrev) = entries.read_abbreviation()? {
                if abbrev.tag() == gimli::DW_TAG_subprogram {
                    let mut attrs = SubprogramAttrs::create();

                    for spec in abbrev.attributes() {
                        let attr = entries.read_attribute(*spec)?;
                        attrs.set_attr(attr);
                    }

                    let range_match = match self.attrs.ranges_offset {
                        None => true,
                        Some(range_offset) => self.match_range(probe, range_offset),
                    };

                    if attrs.match_pc(probe) || range_match {
                        eprintln!("matched subprogram");
                        return Ok(Some(dw_die_offset));
                    }
                } else {
                    entries.skip_attributes(abbrev.attributes())?;
                }
            }
        }

        Ok(None)
    }
}
