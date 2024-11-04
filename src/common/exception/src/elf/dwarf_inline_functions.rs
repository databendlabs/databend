// // Copyright 2021 Datafuse Labs
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
// use gimli::Attribute;
// use gimli::AttributeValue;
// use gimli::EntriesTreeNode;
// use gimli::RangeListsOffset;
// use gimli::Reader;
// use gimli::Result;
// use gimli::UnitOffset;
// use crate::elf::dwarf::CallLocation;
//
// use crate::elf::dwarf_subprogram::SubprogramAttrs;
// use crate::elf::dwarf_unit::Unit;
// use crate::exception_backtrace_elf::HighPc;
//
// pub struct SubroutineAttrs {
//     high_pc: Option<HighPc>,
//     low_pc: Option<u64>,
//     ranges_offset: Option<RangeListsOffset>,
//     line: Option<u64>,
//     file: Option<u64>,
//     column: Option<u64>,
// }
//
// impl SubroutineAttrs {
//     pub fn create() -> SubroutineAttrs {
//         SubroutineAttrs {
//             line: None,
//             column: None,
//             low_pc: None,
//             high_pc: None,
//             ranges_offset: None,
//         }
//     }
//
//     pub fn set_attr<R: Reader>(&mut self, attr: Attribute<R>) {
//         match attr.name() {
//             gimli::DW_AT_low_pc => {
//                 if let AttributeValue::Addr(value) = attr.value() {
//                     self.low_pc = Some(value);
//                 }
//             }
//             gimli::DW_AT_high_pc => match attr.value() {
//                 AttributeValue::Addr(val) => self.high_pc = Some(HighPc::Addr(val)),
//                 AttributeValue::Udata(val) => self.high_pc = Some(HighPc::Offset(val)),
//                 _ => {}
//             },
//             gimli::DW_AT_ranges => {
//                 if let AttributeValue::RangeListsRef(v) = attr.value() {
//                     self.ranges_offset = Some(RangeListsOffset(v.0));
//                 }
//             }
//             gimli::DW_AT_linkage_name | gimli::DW_AT_MIPS_linkage_name => {
//                 // if let Ok(val) = sections.attr_string(unit, attr.value()) {
//                 //     name = Some(val);
//                 // }
//             }
//             gimli::DW_AT_name => {
//                 // if name.is_none() {
//                 //     name = sections.attr_string(unit, attr.value()).ok();
//                 // }
//             }
//             gimli::DW_AT_abstract_origin | gimli::DW_AT_specification => {
//                 match attr.value() {
//                     AttributeValue::UnitRef(offset) => {
//                         name_entry(file, unit, offset, ctx, sections, recursion_limit)
//                     }
//                     AttributeValue::DebugInfoRef(dr) => {
//                         let (unit, offset) = ctx.find_unit(dr, file)?;
//                         name_entry(file, unit, offset, ctx, sections, recursion_limit)
//                     }
//                     AttributeValue::DebugInfoRefSup(dr) => {
//                         if let Some(sup_sections) = sections.sup.as_ref() {
//                             file = DebugFile::Supplementary;
//                             let (unit, offset) = ctx.find_unit(dr, file)?;
//                             name_entry(file, unit, offset, ctx, sup_sections, recursion_limit)
//                         } else {
//                             Ok(None)
//                         }
//                     }
//                 }
//                 // if name.is_none() {
//                 //     name = name_attr(attr.value(), file, unit, ctx, sections, 16)?;
//                 // }
//             }
//             gimli::DW_AT_call_file => {
//                 if let AttributeValue::FileIndex(idx) = attr.value() {
//                     self.file = Some(idx);
//                 }
//             }
//             gimli::DW_AT_call_line => {
//                 self.line = attr.udata_value();
//             }
//             gimli::DW_AT_call_column => {
//                 self.column = attr.udata_value();
//             }
//             _ => {}
//         }
//     }
//
//     pub fn match_pc(&self, probe: u64) -> bool {
//         match (self.low_pc, self.high_pc) {
//             (Some(low), Some(high)) => {
//                 probe >= low
//                     && match high {
//                     HighPc::Addr(high) => probe < high,
//                     HighPc::Offset(size) => probe < low + size,
//                 }
//             }
//             _ => false,
//         }
//     }
// }
//
// impl<R: Reader> Unit<R> {
//     pub(crate) fn find_inlined_functions(&self, probe: u64, offset: UnitOffset, res: &mut Vec<CallLocation>) -> Result<()> {
//         let tree = self.head.entries_tree(&self.abbreviations, Some(offset))?;
//         self.find_inlined(tree.root()?, probe, res)?;
//         Ok(())
//     }
//
//     fn find_inlined<R: Reader>(
//         &self,
//         mut node: EntriesTreeNode<R>,
//         probe: u64,
//         res: &mut Vec<CallLocation>,
//     ) -> Result<bool> {
//         let mut children = node.children();
//         while let Some(child) = children.next()? {
//             // Searching child.
//             if matches!(
//                 child.entry().tag(),
//                 gimli::DW_TAG_try_block
//                     | gimli::DW_TAG_catch_block
//                     | gimli::DW_TAG_lexical_block
//                     | gimli::DW_TAG_common_block
//                     | gimli::DW_TAG_entry_point
//             ) {
//                 self.find_inlined(child, probe, res)?;
//                 return Ok(true);
//             }
//
//             let mut attrs = child.entry().attrs();
//             let mut subroutine_attrs = SubroutineAttrs::create();
//
//             while let Some(attr) = attrs.next()? {
//                 subroutine_attrs.set_attr(attr);
//             }
//
//             let range_match = match self.attrs.ranges_offset {
//                 None => true,
//                 Some(range_offset) => self.match_range(probe, range_offset),
//             };
//             // TODO:
//             // doesn't match. But we'll keep searching
//             if !subroutine_attrs.match_pc(probe) && !range_match {
//                 return Ok(true);
//             }
//
//             // if subroutine_attrs
//
//             self.find_inlined(node, probe, res)?;
//             return Ok(false);
//         }
//
//         Ok(true)
//     }
// }
