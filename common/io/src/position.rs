#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), target_feature = "sse2"))]
fn position_sse2(buf: &[u8]) -> usize {
    unimplemented!()
}

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), target_feature = "sse2"))]
fn position_sse42(buf: &[u8]) -> usize {
    unimplemented!()
}

#[cfg(all(any(target_arch = "aarch64"), target_feature = "neon"))]
fn position_neon(buf: &[u8]) {
    unsafe {
        use std::arch::aarch64::*;

        let bytes = vld1q_u8(buf.as_ptr());
    }
}

#[inline]
pub fn position1<const positive: bool, const c1: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position2<const positive: bool, const c1: u8, const c2: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position3<const positive: bool, const c1: u8, const c2: u8, const c3: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position4<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position5<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position6<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position7<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, 0, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position8<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, 0, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position9<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, 0, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position10<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, 0, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position11<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, 0, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position12<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8, const c12: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, 0, 0, 0, 0>(buf)
}

#[inline]
pub fn position13<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8, const c12: u8, const c13: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, 0, 0, 0>(buf)
}

#[inline]
pub fn position14<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8, const c12: u8, const c13: u8, const c14: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, 0, 0>(buf)
}

#[inline]
pub fn position15<const positive: bool, const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8, const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8, const c12: u8, const c13: u8, const c14: u8, const c15: u8>(buf: &[u8]) -> usize {
    position16::<positive, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, 0>(buf)
}

#[inline]
pub fn position16<
    const positive: bool,
    const c1: u8, const c2: u8, const c3: u8, const c4: u8, const c5: u8, const c6: u8,
    const c7: u8, const c8: u8, const c9: u8, const c10: u8, const c11: u8, const c12: u8,
    const c13: u8, const c14: u8, const c15: u8, const c16: u8
>(buf: &[u8]) -> usize {
    let mut index = 0;

    while index < buf.len() {
        if positive == (buf[index] == c1
            || (c2 != 0 && buf[index] == c2)
            || (c3 != 0 && buf[index] == c3)
            || (c4 != 0 && buf[index] == c4)
            || (c5 != 0 && buf[index] == c5)
            || (c6 != 0 && buf[index] == c6)
            || (c7 != 0 && buf[index] == c7)
            || (c8 != 0 && buf[index] == c8)
            || (c9 != 0 && buf[index] == c9)
            || (c10 != 0 && buf[index] == c10)
            || (c11 != 0 && buf[index] == c11)
            || (c12 != 0 && buf[index] == c12)
            || (c13 != 0 && buf[index] == c13)
            || (c14 != 0 && buf[index] == c14)
            || (c15 != 0 && buf[index] == c15)
            || (c16 != 0 && buf[index] == c16))
        {
            return index;
        }

        index += 1;
    }

    index
}
