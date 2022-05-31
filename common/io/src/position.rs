use std::arch::aarch64::uint8x16_t;

#[inline]
pub fn position1<const POSITIVE: bool, const C1: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position2<const POSITIVE: bool, const C1: u8, const C2: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position3<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position4<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position5<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position6<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position7<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position8<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position9<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position10<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position11<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position12<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position13<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8, const C13: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, 0, 0, 0>(buf)
}

#[inline]
pub fn position14<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8, const C13: u8, const C14: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, 0, 0>(buf)
}

#[inline]
pub fn position15<const POSITIVE: bool, const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8, const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8, const C13: u8, const C14: u8, const C15: u8>(buf: &[u8]) -> usize {
    position16::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, 0>(buf)
}

#[inline]
#[allow(unreachable_code)]
pub fn position16<
    const POSITIVE: bool,
    const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8,
    const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8,
    const C13: u8, const C14: u8, const C15: u8, const C16: u8
>(buf: &[u8]) -> usize {
    #[cfg(all(any(target_arch = "aarch64"), target_feature = "neon"))]
    {
        return position_neon::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16>(buf);
    }

    position16_from_index::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16>(buf, 0)
}

#[cfg(all(any(target_arch = "aarch64"), target_feature = "neon"))]
pub fn position_neon<
    const POSITIVE: bool,
    const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8,
    const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8,
    const C13: u8, const C14: u8, const C15: u8, const C16: u8
>(buf: &[u8]) -> usize {
    unsafe {
        use std::arch::aarch64::*;

        let mut index = 0;

        while index + 15 < buf.len() {
            let bytes = vld1q_u8(buf.as_ptr());
            let mut res = vceqq_u8(bytes, vdupq_n_u8(C1));

            macro_rules! neno_match {
                ($($name:ident,)*) => {
                    $(if $name != 0 {
                        res = vorrq_u8(res, vceqq_u8(bytes, vdupq_n_u8($name)));
                    })*
                };
            }

            neno_match!(C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,);

            let bit_mask = match POSITIVE {
                true => neno_mm_movemask_epi8(res),
                false => !neno_mm_movemask_epi8(res)
            };

            if bit_mask > 0 {
                return index + (bit_mask.trailing_zeros() as usize);
            }

            index += 16;
        }

        position16_from_index::<POSITIVE, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16>(buf, index)
    }
}

// #[cfg(all(any(target_arch = "x86_64"), target_feature = "sse4.2"))]
// fn position_sse42<
//     const POSITIVE: bool,
//     const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8,
//     const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8,
//     const C13: u8, const C14: u8, const C15: u8, const C16: u8
// >(buf: &[u8]) -> usize {
//     unsafe {
//         use std::arch::x86_64::*;
//         // std::arch::x86_64::
//     }
//     // __m128i
//     // set = _mm_setr_epi8(c01, c02, c03, c04, c05, c06, c07, c08, c09, C10, C11, C12, C13, C14, C15, C16);
//
//     unimplemented!()
// }

#[inline(always)]
fn position16_from_index<
    const POSITIVE: bool,
    const C1: u8, const C2: u8, const C3: u8, const C4: u8, const C5: u8, const C6: u8,
    const C7: u8, const C8: u8, const C9: u8, const C10: u8, const C11: u8, const C12: u8,
    const C13: u8, const C14: u8, const C15: u8, const C16: u8
>(buf: &[u8], begin: usize) -> usize {
    let mut index = begin;
    while index < buf.len() {
        if POSITIVE == (buf[index] == C1
            || (C2 != 0 && buf[index] == C2)
            || (C3 != 0 && buf[index] == C3)
            || (C4 != 0 && buf[index] == C4)
            || (C5 != 0 && buf[index] == C5)
            || (C6 != 0 && buf[index] == C6)
            || (C7 != 0 && buf[index] == C7)
            || (C8 != 0 && buf[index] == C8)
            || (C9 != 0 && buf[index] == C9)
            || (C10 != 0 && buf[index] == C10)
            || (C11 != 0 && buf[index] == C11)
            || (C12 != 0 && buf[index] == C12)
            || (C13 != 0 && buf[index] == C13)
            || (C14 != 0 && buf[index] == C14)
            || (C15 != 0 && buf[index] == C15)
            || (C16 != 0 && buf[index] == C16)) {
            return index;
        }

        index += 1;
    }

    index
}

unsafe fn neno_mm_movemask_epi8(input: uint8x16_t) -> u16 {
    use std::arch::aarch64::*;

    let xr = vec![-7_i8, -6, -5, -4, -3, -2, -1, 0, -7_i8, -6, -5, -4, -3, -2, -1, 0];
    let xr = xr.as_ptr();

    let mask_and = vdupq_n_u8(0x80);
    let mask_shift = vld1q_s8(xr);

    let mut temp = vshlq_u8(vandq_u8(input, mask_and), mask_shift);

    temp = vpaddq_u8(temp, temp);
    temp = vpaddq_u8(temp, temp);
    temp = vpaddq_u8(temp, temp);

    vgetq_lane_u16::<0>(vreinterpretq_u16_u8(temp))
}
