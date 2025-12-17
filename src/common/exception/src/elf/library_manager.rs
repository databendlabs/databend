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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use object::Object;
use once_cell::sync::OnceCell;

use crate::elf::ElfFile;
use crate::elf::dwarf::Dwarf;
use crate::elf::library_loader::LibraryLoader;
use crate::elf::library_symbol::Symbol;
use crate::exception_backtrace::PhysicalAddr;
use crate::exception_backtrace::ResolvedStackFrame;
use crate::exception_backtrace::StackFrame;

pub struct Library {
    pub name: String,
    pub address_begin: usize,
    pub address_end: usize,
    pub elf: Option<Arc<ElfFile>>,
    pub build_id: Option<Arc<Vec<u8>>>,
    library_data: &'static [u8],
}

impl Library {
    pub fn create(name: String, data: *const u8, size: usize) -> Library {
        let build_id = unsafe {
            let data = std::slice::from_raw_parts(data, size);
            match ElfFile::parse(data) {
                Err(_) => None,
                Ok(elf_file) => match elf_file.build_id() {
                    Ok(None) | Err(_) => None,
                    Ok(Some(build)) => Some(Arc::new(build.to_vec())),
                },
            }
        };

        Library {
            name,
            build_id,
            elf: None,
            address_end: 0,
            address_begin: 0,
            // Leak memory
            library_data: unsafe { std::slice::from_raw_parts(data, size) },
        }
    }
    pub fn sort_begin_address(&self, other: &Self) -> Ordering {
        self.address_begin.cmp(&other.address_begin)
    }

    pub fn data(&self) -> &'static [u8] {
        self.library_data
    }

    pub unsafe fn build_id(&self) -> Option<Arc<Vec<u8>>> {
        self.build_id.clone()
    }
}

static INSTANCE: OnceCell<Arc<LibraryManager>> = OnceCell::new();

impl Debug for Library {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Library")
            .field("name", &self.name)
            .field("address_begin", &self.address_begin)
            .field("address_end", &self.address_end)
            .finish()
    }
}

// #[derive(Debug)]
pub struct LibraryManager {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
    executable_build_id: Option<Arc<Vec<u8>>>,
}

impl Debug for LibraryManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibraryManager")
            .field("libraries", &self.libraries)
            .field("symbols", &self.symbols.len())
            .finish()
    }
}

impl LibraryManager {
    fn find_library(&self, addr: usize) -> Option<&Library> {
        self.libraries
            .iter()
            .find(|library| library.address_begin <= addr && addr <= library.address_end)
    }

    fn find_library_by_build_id(&self, build_id: &Arc<Vec<u8>>) -> Option<&Library> {
        for library in &self.libraries {
            if let Some(v) = &library.build_id
                && v == build_id
            {
                return Some(library);
            }
        }

        None
    }

    pub fn to_physical_frames(&self, frames: &[StackFrame]) -> Vec<StackFrame> {
        let mut res = Vec::with_capacity(frames.len());

        for frame in frames {
            let StackFrame::Ip(addr) = frame else {
                res.push(frame.clone());
                continue;
            };

            let Some(library) = self.find_library(*addr) else {
                res.push(StackFrame::PhysicalAddr(PhysicalAddr {
                    physical_addr: 0,
                    library_build_id: None,
                }));
                continue;
            };

            res.push(StackFrame::PhysicalAddr(PhysicalAddr {
                library_build_id: library.build_id.clone(),
                physical_addr: addr - library.address_begin,
            }));
        }

        res
    }

    pub fn resolve_frames<E, F: FnMut(ResolvedStackFrame) -> Result<(), E>>(
        &self,
        frames: &[StackFrame],
        only_address: bool,
        mut f: F,
    ) -> Result<(), E> {
        let mut dwarf_cache = HashMap::with_capacity(self.libraries.len());

        for frame in frames {
            let (library, addr) = match frame {
                StackFrame::Ip(addr) => {
                    let Some(library) = self.find_library(*addr) else {
                        f(ResolvedStackFrame {
                            virtual_address: *addr,
                            physical_address: *addr,
                            symbol: String::from("<unknown>"),
                            inlined: false,
                            file: None,
                            line: None,
                            column: None,
                        })?;

                        continue;
                    };

                    (library, *addr - library.address_begin)
                }
                StackFrame::PhysicalAddr(physical_addr) => {
                    let Some(build_id) = &physical_addr.library_build_id else {
                        f(ResolvedStackFrame {
                            virtual_address: 0,
                            physical_address: physical_addr.physical_addr,
                            symbol: String::from("<unknown>"),
                            inlined: false,
                            file: None,
                            line: None,
                            column: None,
                        })?;

                        continue;
                    };

                    let Some(library) = self.find_library_by_build_id(build_id) else {
                        f(ResolvedStackFrame {
                            virtual_address: 0,
                            physical_address: physical_addr.physical_addr,
                            symbol: String::from("<unknown>"),
                            inlined: false,
                            file: None,
                            line: None,
                            column: None,
                        })?;

                        continue;
                    };

                    (library, physical_addr.physical_addr)
                }
            };

            if !only_address {
                let dwarf = match library.elf.as_ref() {
                    None => &None,
                    Some(elf) => match dwarf_cache.get(&library.name) {
                        Some(v) => v,
                        None => {
                            dwarf_cache.insert(library.name.clone(), Dwarf::create(elf.clone()));
                            dwarf_cache.get(&library.name).unwrap()
                        }
                    },
                };

                if let Some(dwarf) = dwarf {
                    let adjusted_addr = (addr - 1) as u64;

                    if let Ok(locations) = dwarf.find_frames(adjusted_addr) {
                        for location in locations {
                            f(ResolvedStackFrame {
                                virtual_address: 0,
                                physical_address: addr,
                                symbol: location.symbol.unwrap_or("<unknown>".to_string()),
                                inlined: location.is_inlined,
                                file: location.file,
                                line: location.line,
                                column: location.column,
                            })?;
                        }

                        continue;
                    }
                }
            }

            f(ResolvedStackFrame {
                physical_address: addr,
                virtual_address: 0,
                inlined: false,
                symbol: String::from("<unknown>"),
                file: None,
                line: None,
                column: None,
            })?;
        }

        Ok(())
    }

    pub fn executable_build_id(&self) -> Option<Arc<Vec<u8>>> {
        self.executable_build_id.clone()
    }

    pub fn create() -> Arc<LibraryManager> {
        let loader = LibraryLoader::load();
        let (libraries, symbols, build_id) = loader.finalize();
        Arc::new(LibraryManager {
            symbols,
            libraries,
            executable_build_id: build_id.map(Arc::new),
        })
    }

    pub fn instance() -> Arc<LibraryManager> {
        INSTANCE.get_or_init(LibraryManager::create).clone()
    }
}
