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

use std::io;
use std::mem::MaybeUninit;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::stat_buffer::StatBuffer;
use crate::unmarshal::Unmarshal;

const MAX_STACK_BUFFER_LEN: usize = 1024;

pub trait BinaryRead {
    fn read_scalar<V>(&mut self) -> Result<V>
    where V: Unmarshal<V> + StatBuffer;

    fn read_opt_scalar<V>(&mut self) -> Result<Option<V>>
    where V: Unmarshal<V> + StatBuffer;

    fn read_string(&mut self) -> Result<String>;
    fn skip_string(&mut self) -> Result<()>;
    fn read_uvarint(&mut self) -> Result<u64>;

    // in place read
    fn read_to_scalar<V>(&mut self, v: &mut V) -> Result<()>
    where V: Unmarshal<V> + StatBuffer {
        *v = self.read_scalar()?;
        Ok(())
    }

    fn read_to_string(&mut self, v: &mut String) -> Result<()> {
        *v = self.read_string()?;
        Ok(())
    }
    fn read_to_uvarint(&mut self, v: &mut u64) -> Result<()> {
        *v = self.read_uvarint()?;
        Ok(())
    }
}

impl<T> BinaryRead for T
where T: io::Read
{
    fn read_scalar<V>(&mut self) -> Result<V>
    where V: Unmarshal<V> + StatBuffer {
        let mut buffer = V::buffer();
        self.read_exact(buffer.as_mut())?;
        V::try_unmarshal(buffer.as_ref())
    }

    fn read_opt_scalar<V>(&mut self) -> Result<Option<V>>
    where V: Unmarshal<V> + StatBuffer {
        let is_some: u8 = self.read_scalar()?;
        if is_some == 0 {
            return Ok(None);
        }
        let mut buffer = V::buffer();
        self.read_exact(buffer.as_mut())?;
        Ok(Some(V::unmarshal(buffer.as_ref())))
    }

    fn read_string(&mut self) -> Result<String> {
        let str_len = self.read_uvarint()? as usize;
        let mut buffer = vec![0_u8; str_len];
        self.read_exact(buffer.as_mut())?;
        Ok(String::from_utf8(buffer)?)
    }

    fn skip_string(&mut self) -> Result<()> {
        let str_len = self.read_uvarint()? as usize;

        if str_len <= MAX_STACK_BUFFER_LEN {
            unsafe {
                let mut buffer: [MaybeUninit<u8>; MAX_STACK_BUFFER_LEN] =
                    MaybeUninit::uninit().assume_init();
                self.read_exact(
                    &mut *(&mut buffer[..str_len] as *mut [MaybeUninit<u8>] as *mut [u8]),
                )?;
            }
        } else {
            let mut buffer = vec![0_u8; str_len];
            self.read_exact(buffer.as_mut())?;
        }

        Ok(())
    }

    fn read_uvarint(&mut self) -> Result<u64> {
        let mut x = 0_u64;
        let mut s = 0_u32;
        let mut i = 0_usize;
        loop {
            let b: u8 = self.read_scalar()?;

            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err(ErrorCode::Overflow("read_uvarint got overflow byte"));
                }
                return Ok(x | (u64::from(b) << s));
            }

            x |= u64::from(b & 0x7f) << s;
            s += 7;

            i += 1;
        }
    }
}
