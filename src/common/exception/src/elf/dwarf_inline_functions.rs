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

use gimli::{Attribute, EntriesRaw};
use gimli::AttributeValue;
use gimli::EntriesTreeNode;
use gimli::RangeListsOffset;
use gimli::Reader;
use gimli::Result;
use gimli::UnitOffset;
use crate::elf::dwarf::CallLocation;

use crate::elf::dwarf_subprogram::SubprogramAttrs;
use crate::elf::dwarf_unit::Unit;
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
            gimli::DW_AT_low_pc => {
                if let AttributeValue::Addr(value) = attr.value() {
                    self.low_pc = Some(value);
                }
            }
            gimli::DW_AT_high_pc => match attr.value() {
                AttributeValue::Addr(val) => self.high_pc = Some(HighPc::Addr(val)),
                AttributeValue::Udata(val) => self.high_pc = Some(HighPc::Offset(val)),
                _ => {}
            },
            gimli::DW_AT_ranges => {
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
                if self.name.is_none() {
                    if let Ok(Some(v)) = unit.name_attr(attr.value(), 16) {
                        self.name = Some(v);
                    }
                }
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
            AttributeValue::DebugStrRef(offset) => self.debug_str.get_str(offset).ok(),
            AttributeValue::DebugLineStrRef(offset) => self.debug_line_str.get_str(offset).ok(),
            AttributeValue::DebugStrOffsetsIndex(index) => {
                let offset = self.debug_str_offsets.get_str_offset(
                    self.head.format(),
                    self.attrs.str_offsets_base.clone(),
                    index,
                ).ok()?;
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
                        if let Some(val) = self.attr_str(attr.value()) {
                            return Ok(Some(val));
                        }
                    }
                    gimli::DW_AT_name => {
                        name = self.attr_str(attr.value());
                    }
                    gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
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
            AttributeValue::UnitRef(offset) => {
                self.name_entry(offset, recursion)
            }
            AttributeValue::DebugInfoRef(dr) => {
                if let Ok(head) = self.debug_info.header_from_offset(dr) {
                    if let Some(offset) = dr.to_unit_offset(&head) {
                        return self.name_entry(offset, recursion);
                    }
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn find_inlined_functions(&self, probe: u64, offset: UnitOffset<R::Offset>, res: &mut Vec<CallLocation>) -> Result<()> {
        let mut tree = self.head.entries_tree(&self.abbreviations, Some(offset))?;
        self.find_inlined(tree.root()?, probe, res)?;
        Ok(())
    }

    fn find_inlined(&self, mut node: EntriesTreeNode<R>, probe: u64, res: &mut Vec<CallLocation>) -> Result<()> {
        let mut children = node.children();
        while let Some(child) = children.next()? {
            // Searching child.
            if matches!(
                child.entry().tag(),
                gimli::DW_TAG_try_block
                    | gimli::DW_TAG_catch_block
                    | gimli::DW_TAG_lexical_block
                    | gimli::DW_TAG_common_block
                    | gimli::DW_TAG_entry_point
            ) {
                self.find_inlined(child, probe, res)?;
                continue;
            }

            let mut attrs = child.entry().attrs();
            let mut subroutine_attrs = SubroutineAttrs::<R>::create();

            while let Some(attr) = attrs.next()? {
                subroutine_attrs.set_attr(attr, self);
            }

            let range_match = match self.attrs.ranges_offset {
                None => true,
                Some(range_offset) => self.match_range(probe, range_offset),
            };

            // doesn't match. But we'll keep searching
            if !subroutine_attrs.match_pc(probe) && !range_match {
                continue;
            }

            eprintln!("matched inlined function");
            if let Some(name) = &subroutine_attrs.name {
                eprintln!("matched inlined function has name");
                if let Ok(name) = name.to_string_lossy() {
                    eprintln!("matched inlined function has nor name");
                    if let Ok(name) = rustc_demangle::try_demangle(name.as_ref()) {
                        eprintln!("matched inlined function has demangle name");
                        if let Some(call_file) = subroutine_attrs.file {
                            // if let Some(lines) = frames.unit.parse_lines(frames.sections)? {
                            //     next.file = lines.files.get(call_file as usize).map(String::as_str);
                            // }
                        }

                        res.push(CallLocation {
                            symbol: Some(format!("{:#}", name)),
                            file: None,
                            line: subroutine_attrs.line,
                            column: subroutine_attrs.column,
                        });
                    }
                }
            }

            self.find_inlined(child, probe, res)?;
            return Ok(());
        }

        Ok(())
    }

    fn inlined_functions(&self, mut entries: EntriesRaw<R>, probe: u64, depth: isize) -> Result<()> {
        loop {
            let next_depth = entries.next_depth();

            if next_depth <= depth {
                return Ok(());
            }

            if let Some(abbrev) = entries.read_abbreviation()? {
                match abbrev.tag() {
                    gimli::DW_TAG_subprogram => {
                        entries.skip_attributes(abbrev.attributes())?;
                        while entries.next_depth() > depth {
                            if let Some(abbrev) = entries.read_abbreviation()? {
                                entries.skip_attributes(abbrev.attributes())?;
                            }
                        }
                    }
                    gimli::DW_TAG_inlined_subroutine => {
                        let mut attrs = SubroutineAttrs::create();
                        for spec in abbrev.attributes() {
                            let attr = entries.read_attribute(*spec)?;
                            attrs.set_attr(attr, self);
                        }

                        let range_match = match self.attrs.ranges_offset {
                            None => true,
                            Some(range_offset) => self.match_range(probe, range_offset),
                        };

                        if attrs.match_pc(probe) || range_match {
                            eprintln!("matched inlined function");
                            if let Some(name) = &attrs.name {
                                eprintln!("matched inlined function has name {:?} {:?} {:?}", name, attrs.line, attrs.column);
                                // if let Ok(name) = name.to_string_lossy() {
                                //     eprintln!("matched inlined function has nor name");
                                //     if let Ok(name) = rustc_demangle::try_demangle(name.as_ref()) {
                                //         eprintln!("matched inlined function has demangle name");
                                //         if let Some(call_file) = subroutine_attrs.file {
                                //             // if let Some(lines) = frames.unit.parse_lines(frames.sections)? {
                                //             //     next.file = lines.files.get(call_file as usize).map(String::as_str);
                                //             // }
                                //         }
                                //
                                //         res.push(CallLocation {
                                //             symbol: Some(format!("{:#}", name)),
                                //             file: None,
                                //             line: subroutine_attrs.line,
                                //             column: subroutine_attrs.column,
                                //         });
                                //     }
                                // }
                            }

                            self.inlined_functions(entries, probe, next_depth)?;

                            return Ok(());
                        }
                    }
                    _ => {
                        entries.skip_attributes(abbrev.attributes())?;
                    }
                }
            }
        }
    }

    pub fn find_function(&self, offset: UnitOffset<R::Offset>, probe: u64, res: &mut Vec<CallLocation>) -> Result<()> {
        let mut entries = self.head.entries_raw(&self.abbreviations, Some(offset))?;
        let depth = entries.next_depth();
        let abbrev = entries.read_abbreviation()?.unwrap();
        debug_assert_eq!(abbrev.tag(), gimli::DW_TAG_subprogram);

        // let mut name = None;
        for spec in abbrev.attributes() {
            let attr = entries.read_attribute(*spec)?;
            match attr.name() {
                gimli::DW_AT_linkage_name | gimli::DW_AT_MIPS_linkage_name => {
                    // if let Ok(val) = sections.attr_string(unit, attr.value()) {
                    //     name = Some(val);
                    // }
                }
                gimli::DW_AT_name => {
                    // if name.is_none() {
                    //     name = sections.attr_string(unit, attr.value()).ok();
                    // }
                }
                gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
                    // if name.is_none() {
                    //     name = name_attr(attr.value(), file, unit, ctx, sections, 16)?;
                    // }
                }
                _ => {}
            };
        }

        self.inlined_functions(entries, probe, depth)
    }
}
