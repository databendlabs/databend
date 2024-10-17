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


use core::slice;
use std::path::PathBuf;
use libc::iovec;
use object::read::elf::{FileHeader, Sym};
use crate::exception_backtrace::{ResolvedStackFrame, StackFrame};

#[cfg(target_pointer_width = "32")]
type Elf = object::elf::FileHeader32<object::NativeEndian>;
#[cfg(target_pointer_width = "64")]
type Elf = object::elf::FileHeader64<object::NativeEndian>;

struct Symbol {
    name: &'static [u8],
    address_begin: u64,
    address_end: u64,
}

struct Library {
    name: String,
    address_begin: usize,
    address_end: usize,
    library_data: Vec<u8>,
    // std::shared_ptr<Elf> elf;
}

struct LibraryManager {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
}

impl LibraryManager {
    fn find_library(&self, addr: usize) -> Option<&Library> {
        match self.libraries.iter().find(|library| library.address_begin <= addr) {
            None => None,
            Some(v) => match v.address_end >= addr {
                true => Some(v),
                false => None,
            }
        }
    }

    fn find_symbol(&self, addr: usize) -> Option<&Symbol> {
        match self.symbols.iter().find(|symbol| symbol.address_begin as usize <= addr) {
            None => None,
            Some(v) => match v.address_end as usize >= addr {
                true => Some(v),
                false => None,
            }
        }
    }

    pub fn resolve_frames(&self, frames: Vec<StackFrame>) -> Vec<ResolvedStackFrame> {
        let mut res = Vec::with_capacity(frames.len());
        for frame in &mut frames {
            if let StackFrame::Unresolved(addr) = frame {
                let Some(library) = self.find_library(*addr) else {
                    res.push(ResolvedStackFrame {
                        virtual_address: *addr,
                        physical_address: *addr,
                        library: String::new(),
                        symbol: String::from("<unknown>"),
                    });

                    continue;
                };

                if let Some(symbol) = self.find_symbol(*addr) {
                    res.push(ResolvedStackFrame {
                        virtual_address: *addr,
                        physical_address: *addr - library.address_begin,
                        library: String::new(),
                        symbol: format!("{}", rustc_demangle::demangle(std::str::from_utf8(symbol.name).unwrap())),
                    });
                }
            }
        }

        res
    }

    #[cfg(target_os = "linux")]
    pub fn create(&mut self) -> LibraryManager {
        unsafe {
            let mut data = Data { symbols: vec![], libraries: vec![] };
            libc::dl_iterate_phdr(Some(callback), core::ptr::addr_of_mut!(data).cast());

            data.symbols.sort_by(|a, b| a.address_begin.cmp(&b.address_begin));
            data.libraries.sort_by(|a, b| a.address_begin.cmp(&b.address_begin));
            data.symbols.dedup_by(|a, b| a.address_begin == b.address_begin && b.address_end == b.address_end);

            LibraryManager {
                symbols: data.symbols,
                libraries: data.libraries,
            }
        }
    }

    pub fn debug_path(library_name: &str, build_id: &[u8]) -> std::io::Result<PathBuf> {
        let mut binary_path = std::fs::canonicalize(library_name)?.to_path_buf();

        let mut binary_debug_path = binary_path.clone();
        binary_debug_path.set_extension("debug");

        if std::fs::exists(&binary_debug_path)? {
            return Ok(binary_debug_path);
        }

        let relative_path = binary_path.strip_prefix("/").unwrap();
        let mut system_named_debug_path =
            std::path::Path::new("/uar/lib/debug").join(relative_path);
        system_named_debug_path.set_extension("debug");

        if std::fs::exists(&system_named_debug_path)? {
            return Ok(system_named_debug_path);
        }

        if build_id.len() >= 2 {
            let mut system_dir = std::path::Path::new("/uar/lib/debug").to_path_buf();

            fn encode_hex(bytes: &[u8]) -> String {
                let mut encoded_hex = String::with_capacity(bytes.len() * 2);
                for &b in bytes {
                    write!(&mut encoded_hex, "{:02x}", b).unwrap();
                }
                encoded_hex
            }

            let mut system_id_debug_path = system_dir
                .join(encode_hex(&build_id[..1]))
                .join(encode_hex(&build_id[1..]));

            system_id_debug_path.set_extension("debug");

            if std::fs::exists(&system_id_debug_path)? {
                return Ok(system_id_debug_path);
            }
        }

        Ok(binary_path)
    }

    // pub fn resolve_frames(frames: &mut Vec<StackFrame>, symbols: &[Symbol]) {
    //     for stack_frame in frames {
    //         // if let StackFrame::UnSymbol(addr) = stack_frame {
    //         // let Some(symbol) = symbols.iter().position(
    //         //     |symbol| symbol.start_address as usize <= *addr
    //         // ) else {
    //         //     // *stack_frame = StackFrame::Unknown()
    //         // };
    //         // }
    //     }
    // }
}

struct Data {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
}

#[cfg(target_os = "linux")]
unsafe extern "C" fn callback(info: *mut libc::dl_phdr_info, _size: libc::size_t, vec: *mut libc::c_void) -> libc::c_int {
    let info = &*info;
    let data = &mut *vec.cast::<Data>();
    let is_main_prog = info.dlpi_name.is_null() || *info.dlpi_name == 0;
    let name = if is_main_prog {
        // The man page for dl_iterate_phdr says that the first object visited by
        // callback is the main program; so the first time we encounter a
        // nameless entry, we can assume its the main program and try to infer its path.
        // After that, we cannot continue that assumption, and we use an empty string.
        // if libs.is_empty() {
        //     infer_current_exe(info.dlpi_addr as usize)
        // } else {
        //     OsString::new()
        // }
    } else {
        // let bytes = CStr::from_ptr(info.dlpi_name).to_bytes();
        // OsStr::from_bytes(bytes).to_owned()
    };

    let library = Library {
        name: "".to_string(),
        address_begin: info.dlpi_addr,
        address_end: 0,
        // address_end: info.dlpi_addr + info,
        library_data: vec![],
    };

    symbols_from_elf(library.address_begin, &library.library_data, &mut data.symbols);
    data.libraries.push(library);
    0
}

pub fn symbols_from_elf(base: u64, data: &[u8], symbols: &mut Vec<Symbol>) -> bool {
    let Ok(elf) = Elf::parse(data) else {
        return false;
    };

    let Ok(endian) = elf.endian() else {
        return false;
    };

    let Ok(sections) = elf.sections(endian, data) else {
        return false;
    };

    let symbol_table = match sections.symbols(endian, data, object::elf::SHT_SYMTAB) {
        Ok(st) => st,
        Err(_) => match sections.symbols(endian, data, object::elf::SHT_DYNSYM) {
            Ok(st) => st,
            Err(_) => {
                return false;
            }
        }
    };

    let string_table = symbol_table.strings();
    for (_idx, symbol) in symbol_table.enumerate() {
        let Ok(sym_name) = symbol.name(endian, string_table) else {
            continue;
        };

        if sym_name.is_empty() {
            continue;
        }

        symbols.push(Symbol {
            name: unsafe { std::mem::transmute(sym_name) },
            address_begin: base + symbol.st_value(endian),
            address_end: base + symbol.st_value(endian) + symbol.st_size(endian),
        })
    }

    true
}