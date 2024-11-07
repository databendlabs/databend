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
use gimli::DebugInfoOffset;
use gimli::EntriesRaw;
use gimli::EntriesTreeNode;
use gimli::RangeListsOffset;
use gimli::Reader;
use gimli::ReaderOffset;
use gimli::Result;
use gimli::UnitOffset;

use crate::elf::dwarf::CallLocation;
use crate::elf::dwarf_subprogram::SubprogramAttrs;
use crate::elf::dwarf_unit::Unit;
use crate::elf::dwarf_unit::UnitAttrs;
use crate::exception_backtrace_elf::HighPc;

pub struct SubroutineAttrs<R: Reader> {
    high_pc: Option<HighPc>,
    low_pc: Option<u64>,
    ranges_offset: Option<RangeListsOffset<R::Offset>>,

    name: Option<R>,
    line: Option<u32>,
    file: Option<u64>,
    column: Option<u32>,
}

impl<R: Reader> SubroutineAttrs<R> {
    pub fn create() -> SubroutineAttrs<R> {
        SubroutineAttrs {
            line: None,
            file: None,
            name: None,
            column: None,
            low_pc: None,
            high_pc: None,
            ranges_offset: None,
        }
    }

    pub fn set_attr(&mut self, attr: Attribute<R>, unit: &Unit<R>) {
        match attr.name() {
            gimli::DW_AT_low_pc => match attr.value() {
                AttributeValue::DebugAddrIndex(idx) => self.low_pc = Some(unit.get_address(idx)),
                AttributeValue::Addr(value) => self.low_pc = Some(value),
                _ => {}
            },
            gimli::DW_AT_high_pc => match attr.value() {
                AttributeValue::Addr(val) => self.high_pc = Some(HighPc::Addr(val)),
                AttributeValue::Udata(val) => self.high_pc = Some(HighPc::Offset(val)),
                AttributeValue::DebugAddrIndex(idx) => {
                    self.high_pc = Some(HighPc::Addr(unit.get_address(idx)))
                }
                _ => {}
            },
            gimli::DW_AT_ranges => {
                // match attr.value() {
                //     AttributeValue::RangeListsRef(offset) => {
                //         self.ranges_offset =
                //         Ok(Some(self.ranges_offset_from_raw(unit, offset)))
                //     }
                //     AttributeValue::DebugRngListsIndex(index) => self.ranges_offset(unit, index).map(Some),
                //     _ => Ok(None),
                // }

                if let AttributeValue::RangeListsRef(v) = attr.value() {
                    self.ranges_offset = Some(RangeListsOffset(v.0));
                }
            }
            gimli::DW_AT_linkage_name | gimli::DW_AT_MIPS_linkage_name => {
                if let Some(val) = unit.attr_str(attr.value()) {
                    self.name = Some(val);
                }
            }
            gimli::DW_AT_name => {
                if self.name.is_none() {
                    self.name = unit.attr_str(attr.value());
                }
            }
            gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
                eprintln!(
                    "gimli::DW_AT_abstract_origin | gimli::DW_AT_specification before {:?} {:?}",
                    self.name.as_ref().map(|x| x.to_string_lossy()), attr.value(),
                );
                if self.name.is_none() {
                    if let Ok(Some(v)) = unit.name_attr(attr.value(), 16) {
                        self.name = Some(v);
                    }
                }
                eprintln!(
                    "gimli::DW_AT_abstract_origin | gimli::DW_AT_specification after {:?}",
                    self.name.as_ref().map(|x| x.to_string_lossy())
                );
            }
            gimli::DW_AT_call_file => {
                if let AttributeValue::FileIndex(idx) = attr.value() {
                    self.file = Some(idx);
                }
            }
            gimli::DW_AT_call_line => {
                self.line = attr.udata_value().map(|x| x as u32);
            }
            gimli::DW_AT_call_column => {
                self.column = attr.udata_value().map(|x| x as u32);
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
    pub(crate) fn attr_str(&self, value: AttributeValue<R>) -> Option<R> {
        match value {
            AttributeValue::String(string) => Some(string),
            AttributeValue::DebugStrRef(offset) => {
                eprintln!("attr_str DebugStrRef {:?}", offset);
                self.debug_str.get_str(offset).ok()
            }
            AttributeValue::DebugLineStrRef(offset) => {
                eprintln!("attr_str DebugLineStrRef {:?}", offset);
                self.debug_line_str.get_str(offset).ok()
            }
            AttributeValue::DebugStrOffsetsIndex(index) => {
                eprintln!("attr_str DebugStrOffsetsIndex {:?}", index);
                let offset = self
                    .debug_str_offsets
                    .get_str_offset(
                        self.head.format(),
                        self.attrs.str_offsets_base.clone(),
                        index,
                    )
                    .ok()?;
                self.debug_str.get_str(offset).ok()
            }
            _ => None,
        }
    }

    fn name_entry(&self, offset: UnitOffset<R::Offset>, recursion: usize) -> Result<Option<R>> {
        let mut entries = self.head.entries_raw(&self.abbreviations, Some(offset))?;
        let abbrev = if let Some(abbrev) = entries.read_abbreviation()? {
            abbrev
        } else {
            return Err(gimli::Error::NoEntryAtGivenOffset);
        };

        let mut name = None;
        let mut next = None;
        for spec in abbrev.attributes() {
            match entries.read_attribute(*spec) {
                Ok(ref attr) => match attr.name() {
                    gimli::DW_AT_linkage_name | gimli::DW_AT_MIPS_linkage_name => {
                        eprintln!("DW_AT_linkage_name | DW_AT_MIPS_linkage_name");
                        if let Some(val) = self.attr_str(attr.value()) {
                            return Ok(Some(val));
                        }
                    }
                    gimli::DW_AT_name => {
                        eprintln!("DW_AT_name");
                        name = self.attr_str(attr.value());
                    }
                    gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
                        eprintln!("gimli::DW_AT_abstract_origin | gimli::DW_AT_specification {}", attr.name());
                        next = Some(attr.value());
                    }
                    _ => {}
                },
                Err(e) => return Err(e),
            }
        }

        if name.is_some() {
            return Ok(name);
        }

        if let Some(next) = next {
            return self.name_attr(next, recursion - 1);
        }

        Ok(None)
    }

    pub(crate) fn name_attr(&self, v: AttributeValue<R>, recursion: usize) -> Result<Option<R>> {
        if recursion == 0 {
            return Ok(None);
        }

        match v {
            AttributeValue::UnitRef(offset) => self.name_entry(offset, recursion),
            AttributeValue::DebugInfoRef(dr) => {
                let mut offset = DebugInfoOffset(R::Offset::from_u8(0));

                let mut head = None;
                let mut units = self.debug_info.units();

                while let Some(unit_head) = units
                    .next()
                    .map_err(|x| gimli::Error::NoEntryAtGivenOffset)?
                {
                    if unit_head.offset().as_debug_info_offset().unwrap() > dr {
                        break;
                    }

                    head = Some(unit_head);
                }

                if let Some(head) = head {
                    let unit_offset = dr
                        .to_unit_offset(&head)
                        .ok_or(gimli::Error::NoEntryAtGivenOffset)?;

                    eprintln!("debug info ref offset {:?}, unit offset {:?} entities offset {:?}", dr, head.offset(), unit_offset);

                    let abbrev_offset = head.debug_abbrev_offset();
                    let Ok(abbreviations) = self.debug_abbrev.abbreviations(abbrev_offset) else {
                        return Ok(None);
                    };

                    let mut cursor = head.entries(&abbreviations);
                    let (_idx, root) = cursor.next_dfs()?.unwrap();

                    let mut attrs = root.attrs();
                    let mut unit_attrs = UnitAttrs::create();

                    while let Some(attr) = attrs.next()? {
                        unit_attrs.set_attr(&self.debug_str, attr);
                    }

                    let unit = Unit {
                        head,
                        abbreviations,
                        attrs: unit_attrs,
                        debug_str: self.debug_str.clone(),
                        debug_info: self.debug_info.clone(),
                        debug_abbrev: self.debug_abbrev.clone(),
                        debug_line: self.debug_line.clone(),
                        debug_line_str: self.debug_line_str.clone(),
                        debug_str_offsets: self.debug_str_offsets.clone(),
                        debug_addr: self.debug_addr.clone(),
                        range_list: self.range_list.clone(),
                    };

                    return unit.name_entry(unit_offset, recursion);
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn inlined_functions(
        &self,
        mut entries: EntriesRaw<R>,
        probe: u64,
        depth: isize,
        res: &mut Vec<CallLocation>,
    ) -> Result<()> {
        loop {
            let next_depth = entries.next_depth();

            if next_depth <= depth {
                return Ok(());
            }

            if let Some(abbrev) = entries.read_abbreviation()? {
                match abbrev.tag() {
                    gimli::DW_TAG_subprogram => {
                        eprintln!("inlined is subprogram");
                        entries.skip_attributes(abbrev.attributes())?;
                        while entries.next_depth() > next_depth {
                            if let Some(abbrev) = entries.read_abbreviation()? {
                                entries.skip_attributes(abbrev.attributes())?;
                            }
                        }
                    }
                    gimli::DW_TAG_inlined_subroutine => {
                        eprintln!("inlined is subprogram DW_TAG_inlined_subroutine");
                        let mut attrs = SubroutineAttrs::create();
                        for spec in abbrev.attributes() {
                            let attr = entries.read_attribute(*spec)?;
                            attrs.set_attr(attr, self);
                        }

                        let match_range = match attrs.ranges_offset {
                            None => false,
                            Some(range_offset) => self.match_range(probe, range_offset),
                        };

                        if !match_range && !attrs.match_pc(probe) {
                            continue
                        }

                        if let Some(name) = &attrs.name {
                            if let Ok(name) = name.to_string_lossy() {
                                if let Ok(name) = rustc_demangle::try_demangle(name.as_ref()) {
                                    res.push(CallLocation {
                                        symbol: Some(format!("{:#}", name)),
                                        file: None,
                                        line: attrs.line,
                                        column: attrs.column,
                                    });
                                }
                            }
                        }

                        self.inlined_functions(entries, probe, next_depth, res)?;

                        return Ok(());
                    }
                    _ => {
                        eprintln!("inlined is {:?}", abbrev.tag());
                        entries.skip_attributes(abbrev.attributes())?;
                    }
                }
            }
        }
    }

    pub fn find_function(
        &self,
        offset: UnitOffset<R::Offset>,
        probe: u64,
        res: &mut Vec<CallLocation>,
    ) -> Result<()> {
        let mut entries = self.head.entries_raw(&self.abbreviations, Some(offset))?;
        let depth = entries.next_depth();
        let abbrev = entries.read_abbreviation()?.unwrap();
        debug_assert_eq!(abbrev.tag(), gimli::DW_TAG_subprogram);

        let mut name = None;
        for spec in abbrev.attributes() {
            let attr = entries.read_attribute(*spec)?;
            match attr.name() {
                gimli::DW_AT_linkage_name | gimli::DW_AT_MIPS_linkage_name => {
                    if let Some(val) = self.attr_str(attr.value()) {
                        name = Some(val);
                    }
                }
                gimli::DW_AT_name => {
                    if name.is_none() {
                        name = self.attr_str(attr.value());
                    }
                }
                gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
                    if name.is_none() {
                        name = self.name_attr(attr.value(), 16)?;
                    }
                }
                _ => {}
            };
        }

        self.inlined_functions(entries, probe, depth, res)?;

        eprintln!("inline functions: {:?}", res);
        // TODO: find location
        if let Some(name) = name {
            if let Ok(name) = name.to_string_lossy() {
                res.push(CallLocation {
                    symbol: Some(format!("{}", rustc_demangle::demangle(name.as_ref()))),
                    file: None,
                    line: None,
                    column: None,
                })
            }
        }

        // res.push(CallLocation {
        //     symbol: name,
        //     file: None,
        //     line: None,
        //     column: None,
        // });

        Ok(())
    }
}


