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

//! Vectorised bit-packing utilities

macro_rules! impl_bitpack {
    ($name:ident,$unpack_name:ident,$t:ty,$bits:literal) => {
        pub fn $name(input: &[$t;256], output: &mut [u8], width: usize) {
            seq_macro::seq!(i in 0..= $bits{
                if i == width{
                    return $name::pack::<i>(input,output);
                }
            });
            panic!("invalid width {}", width);
        }

        pub fn $unpack_name(input: &[u8], output: &mut [$t], width: usize) {
            seq_macro::seq!(i in 0..=$bits{
                if i == width{
                    return $name::unpack::<i>(input, output);
                }
            });
            panic!("invalid width {}", width);
        }

        mod $name {
            #![allow(unused_assignments)]
            pub fn pack<const WIDTH: usize>(input: &[$t;256], output: &mut [u8]) {
                unsafe{
                    let output: &mut [$t] = bytemuck::cast_slice_mut(output);
                    let mut o_idx = 0;
                    const CHUNK_SIZE: usize = 256 / $bits as usize;
                    seq_macro::seq!(chunk_idx in 0..$bits{
                        let i_chunk = &input[chunk_idx * CHUNK_SIZE..];
                        let shift = (chunk_idx * WIDTH) % $bits;
                        let msb = shift + WIDTH;
                        if shift == 0 {
                            for i in 0..CHUNK_SIZE {
                                *output.get_unchecked_mut(o_idx + i) = *i_chunk.get_unchecked(i);
                            }
                        } else if msb < $bits {
                            for i in 0..CHUNK_SIZE {
                                    *output.get_unchecked_mut(o_idx + i) |= *i_chunk.get_unchecked(i) << shift;
                            }
                        } else if msb == $bits {
                            for i in 0..CHUNK_SIZE {
                                *output.get_unchecked_mut(o_idx + i) |= *i_chunk.get_unchecked(i) << shift;
                            }
                            o_idx += CHUNK_SIZE;
                        } else {
                            for i in 0..CHUNK_SIZE {
                                *output.get_unchecked_mut(o_idx + i) |= *i_chunk.get_unchecked(i) << shift;
                            }
                            o_idx += CHUNK_SIZE;
                            for i in 0..CHUNK_SIZE {
                                    *output.get_unchecked_mut(o_idx + i) =
                                        *i_chunk.get_unchecked(i) >> ($bits - shift);
                            }
                        }
                    });
                }
            }
            pub fn unpack<const WIDTH: usize>(input: &[u8], output: &mut [$t]) {
                unsafe {
                    let input: &[$t] = bytemuck::cast_slice(input);
                    let mut i_idx = 0;
                    let mask = (1 << WIDTH) - 1;
                    const CHUNK_SIZE: usize = 256 / $bits as usize;
                    seq_macro::seq!(chunk_idx in 0..$bits{
                        let o_chunk = & mut output[chunk_idx * CHUNK_SIZE..];
                        let shift = (chunk_idx * WIDTH) % $bits;
                        if shift + WIDTH < $bits {
                            for i in 0..CHUNK_SIZE {
                                    *o_chunk.get_unchecked_mut(i) =
                                        (*input.get_unchecked(i_idx + i) >> shift) & mask;
                            }
                        } else {
                            for i in 0..CHUNK_SIZE {
                                 *o_chunk.get_unchecked_mut(i) = *input.get_unchecked(i_idx + i) >> shift ;
                            }
                            i_idx += CHUNK_SIZE;
                            let overflow_mask = (1 << shift + WIDTH - $bits) - 1;
                            for i in 0..CHUNK_SIZE {
                                    *o_chunk.get_unchecked_mut(i) |=
                                        (*input.get_unchecked(i_idx + i) & overflow_mask) << ($bits - shift);
                            }
                        }
                    });
                }
            }
        }
    };
}

impl_bitpack!(pack32, unpack32, u32, 32);
impl_bitpack!(pack64, unpack64, u64, 64);

pub fn need_bytes(num_elem: usize, width: u8) -> usize {
    num_elem * width as usize / 8
}

#[cfg(test)]
mod tests {
    use bitpacking::BitPacker;
    use bitpacking::BitPacker8x;

    use super::*;
    const GROUP_NUM: usize = 10000;
    const WIDTH: u8 = 8;

    #[test]
    fn test_bitpacking() {
        let data = (0..256).collect::<Vec<_>>();
        let need_bytes = need_bytes(data.len(), WIDTH);
        let mut output = vec![0; need_bytes * GROUP_NUM];
        let bitpacker = BitPacker8x::new();
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            bitpacker.compress(&data, &mut output[i * need_bytes..], WIDTH);
        }
        println!("encode(bitpacking): {:?}", quanta::Instant::now() - start);
        let mut decoded = vec![0; data.len()];
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            bitpacker.decompress(&output[i * need_bytes..], &mut decoded, WIDTH);
        }
        println!("decode(bitpacking): {:?}", quanta::Instant::now() - start);
        for (expected, actual) in data.iter().zip(decoded.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_performance_simd_unroll() {
        let data = (0u32..256).collect::<Vec<_>>();
        let need_bytes = need_bytes(data.len(), WIDTH);
        let mut output = vec![0; need_bytes * GROUP_NUM];
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            pack32(
                &data[0..256].try_into().unwrap(),
                &mut output[i * need_bytes..],
                WIDTH as usize,
            );
        }
        println!(
            "encode(simd + unroll): {:?}",
            quanta::Instant::now() - start
        );
        let mut decoded = vec![0; data.len()];
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            unpack32(&output[i * need_bytes..], &mut decoded, WIDTH as usize);
        }
        println!(
            "decode(simd + unroll): {:?}",
            quanta::Instant::now() - start
        );
        for (expected, actual) in data.iter().zip(decoded.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_performance_simd_unroll_64() {
        let data = (0u64..256).collect::<Vec<_>>();
        let need_bytes = need_bytes(data.len(), WIDTH);
        let mut output = vec![0; need_bytes * GROUP_NUM];
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            pack64(
                &data[0..256].try_into().unwrap(),
                &mut output[i * need_bytes..],
                WIDTH as usize,
            );
        }
        println!(
            "encode_64(simd + unroll): {:?}",
            quanta::Instant::now() - start
        );
        let mut decoded = vec![0; data.len()];
        let start = quanta::Instant::now();
        for i in 0..GROUP_NUM {
            unpack64(&output[i * need_bytes..], &mut decoded, WIDTH as usize);
        }
        println!(
            "decode_64(simd + unroll): {:?}",
            quanta::Instant::now() - start
        );
        for (expected, actual) in data.iter().zip(decoded.iter()) {
            assert_eq!(expected, actual);
        }
    }
}
