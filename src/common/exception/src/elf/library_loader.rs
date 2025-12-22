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

use std::ffi::CStr;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::Write;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use libc::c_int;
use libc::c_void;
use libc::dl_iterate_phdr;
use libc::dl_phdr_info;
use libc::size_t;
use object::Object;
use object::ObjectSymbol;
use object::ObjectSymbolTable;

use crate::elf::ElfFile;
use crate::elf::library_manager::Library;
use crate::elf::library_symbol::Symbol;

#[derive(Debug)]
pub struct LibraryLoader {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
}

impl LibraryLoader {
    pub fn load() -> LibraryLoader {
        unsafe {
            let mut loader = LibraryLoader {
                symbols: vec![],
                libraries: vec![],
            };

            dl_iterate_phdr(Some(callback), std::ptr::addr_of_mut!(loader).cast());

            loader
        }
    }

    pub unsafe fn load_symbols(&mut self, library: &mut Library) {
        let library_data = library.data();

        let Ok(elf) = ElfFile::parse(library_data) else {
            return;
        };

        let symbol_table = match elf.symbol_table() {
            Some(symbol_table) => symbol_table,
            None => match elf.dynamic_symbol_table() {
                Some(dynamic_symbol_table) => dynamic_symbol_table,
                None => {
                    return;
                }
            },
        };

        for symbol in symbol_table.symbols() {
            let Ok(sym_name) = symbol.name() else {
                continue;
            };

            if sym_name.is_empty() || symbol.size() == 0 {
                continue;
            }

            self.symbols.push(Symbol {
                #[allow(clippy::missing_transmute_annotations)]
                name: unsafe { std::mem::transmute(sym_name) },
                address_begin: library.address_begin as u64 + symbol.address(),
                address_end: library.address_begin as u64 + symbol.address() + symbol.size(),
            });
        }

        library.elf = Some(Arc::new(elf));
    }

    unsafe fn mmap_library(&mut self, library_path: PathBuf) -> std::io::Result<Library> {
        let name = format!("{}", library_path.display());
        let file = std::fs::File::open(library_path)?;
        let file_len = file.metadata()?.len();
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                file_len as libc::size_t,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };

        match std::ptr::eq(ptr, libc::MAP_FAILED) {
            true => Err(std::io::Error::other("Cannot mmap")),
            false => Ok(Library::create(name, ptr as *const u8, file_len as usize)),
        }
    }

    unsafe fn load_library(&mut self, info: &dl_phdr_info) -> std::io::Result<Library> {
        let library_name = match info.dlpi_name.is_null() || unsafe { *info.dlpi_name } == 0 {
            true => match self.libraries.is_empty() {
                true => OsString::from("/proc/self/exe"),
                false => OsString::new(),
            },
            false => {
                let bytes = unsafe { CStr::from_ptr(info.dlpi_name) }.to_bytes();
                OsStr::from_bytes(bytes).to_owned()
            }
        };

        if library_name.is_empty() {
            return Err(std::io::Error::other("empty library name"));
        }

        let binary_path = std::fs::canonicalize(library_name)?.to_path_buf();
        let mut binary_library = unsafe { self.mmap_library(binary_path.clone())? };

        let Some(binary_build_id) = (unsafe { binary_library.build_id() }) else {
            let binary_data = binary_library.data();
            binary_library.address_begin = info.dlpi_addr as usize;
            binary_library.address_end = info.dlpi_addr as usize + binary_data.len();
            return Ok(binary_library);
        };

        // Symbol binary in the same dir ./databend-query.debug
        let mut binary_debug_path = binary_path.clone();
        binary_debug_path.set_extension("debug");

        if std::fs::exists(&binary_debug_path)? {
            let mut library = unsafe { self.mmap_library(binary_debug_path)? };
            if matches!(unsafe { library.build_id() }, Some(v) if v == binary_build_id) {
                let library_data = library.data();
                library.address_begin = info.dlpi_addr as usize;
                library.address_end = info.dlpi_addr as usize + library_data.len();
                return Ok(library);
            }
        }

        // Symbol binary in the system lib/debug/relative_path dir
        // /usr/lib/debug/home/ubuntu/databend/databend-query.debug
        let Ok(relative_path) = binary_path.strip_prefix("/") else {
            return Err(std::io::Error::other("Cannot strip_prefix for path"));
        };

        let lib_debug = std::path::Path::new("/usr/lib/debug");
        let mut system_named_debug_path = lib_debug.join(relative_path);
        system_named_debug_path.set_extension("debug");

        if std::fs::exists(&system_named_debug_path)? {
            let mut library = unsafe { self.mmap_library(system_named_debug_path)? };
            if matches!(unsafe { library.build_id() }, Some(v) if v == binary_build_id) {
                let library_data = library.data();
                library.address_begin = info.dlpi_addr as usize;
                library.address_end = info.dlpi_addr as usize + library_data.len();
                return Ok(library);
            }
        }

        if binary_build_id.len() >= 2 {
            fn encode_hex(bytes: &[u8]) -> String {
                let mut encoded_hex = String::with_capacity(bytes.len() * 2);
                for &b in bytes {
                    write!(&mut encoded_hex, "{:02x}", b).unwrap();
                }
                encoded_hex
            }

            let mut system_id_debug_path = lib_debug
                .join(encode_hex(&binary_build_id[..1]))
                .join(encode_hex(&binary_build_id[1..]));

            system_id_debug_path.set_extension("debug");

            if std::fs::exists(&system_id_debug_path)? {
                let mut library = unsafe { self.mmap_library(system_id_debug_path)? };

                if matches!(unsafe { library.build_id() }, Some(v) if v == binary_build_id) {
                    let library_data = library.data();
                    library.address_begin = info.dlpi_addr as usize;
                    library.address_end = info.dlpi_addr as usize + library_data.len();
                    return Ok(library);
                }
            }
        }

        let binary_library_data = binary_library.data();
        binary_library.address_begin = info.dlpi_addr as usize;
        binary_library.address_end = info.dlpi_addr as usize + binary_library_data.len();
        Ok(binary_library)
    }

    pub unsafe fn load_libraries(&mut self, info: &dl_phdr_info) {
        if let Ok(mut library) = unsafe { self.load_library(info) } {
            unsafe { self.load_symbols(&mut library) };
            self.libraries.push(library);
        }
    }

    fn executable_build_id(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        unsafe {
            let library_name = OsString::from("/proc/self/exe");
            let binary_path = std::fs::canonicalize(library_name)?.to_path_buf();
            let binary_library = self.mmap_library(binary_path.clone())?;
            Ok(binary_library.build_id().map(|x| x.to_vec()))
        }
    }

    pub fn finalize(mut self) -> (Vec<Library>, Vec<Symbol>, Option<Vec<u8>>) {
        self.symbols.sort_by(Symbol::sort_begin_address);
        self.libraries.sort_by(Library::sort_begin_address);
        self.symbols.dedup_by(Symbol::same_address);

        match self.executable_build_id() {
            Err(_) => (self.libraries, self.symbols, None),
            Ok(build_id) => (self.libraries, self.symbols, build_id),
        }
    }
}

unsafe extern "C" fn callback(info: *mut dl_phdr_info, _size: size_t, v: *mut c_void) -> c_int {
    let loader = unsafe { &mut *v.cast::<LibraryLoader>() };
    unsafe {
        loader.load_libraries(&*info);
    }
    0
}
