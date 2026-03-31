// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::LazyLock;

/// A level of SIMD support for some feature
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdSupport {
    None,
    Neon,
    Sse,
    Avx2,
    Avx512,
    Avx512FP16,
    Lsx,
    Lasx,
}

/// Support for SIMD operations
pub static SIMD_SUPPORT: LazyLock<SimdSupport> = LazyLock::new(|| {
    #[cfg(all(target_arch = "aarch64", any(target_os = "ios", target_os = "tvos")))]
    {
        // AArch64 iOS/tvOS has NEON; fp16 arithmetic is available on modern targets.
        SimdSupport::Neon
    }
    #[cfg(all(
        target_arch = "aarch64",
        not(any(target_os = "ios", target_os = "tvos"))
    ))]
    {
        if aarch64::has_neon_f16_support() {
            SimdSupport::Neon
        } else {
            SimdSupport::None
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if x86::has_avx512() {
            if x86::has_avx512_f16_support() {
                SimdSupport::Avx512FP16
            } else {
                SimdSupport::Avx512
            }
        } else if is_x86_feature_detected!("avx2") {
            SimdSupport::Avx2
        } else {
            SimdSupport::None
        }
    }
    #[cfg(target_arch = "loongarch64")]
    {
        if loongarch64::has_lasx_support() {
            SimdSupport::Lasx
        } else if loongarch64::has_lsx_support() {
            SimdSupport::Lsx
        } else {
            SimdSupport::None
        }
    }
});

#[cfg(target_arch = "x86_64")]
mod x86 {
    use core::arch::x86_64::__cpuid;

    #[inline]
    fn check_flag(x: usize, position: u32) -> bool {
        x & (1 << position) != 0
    }

    pub fn has_avx512_f16_support() -> bool {
        // this macro does many OS checks/etc. to determine if allowed to use AVX512
        if !has_avx512() {
            return false;
        }

        // EAX=7, ECX=0: Extended Features (includes AVX512)
        // More info on calling CPUID can be found here (section 1.4)
        // https://www.intel.com/content/dam/develop/external/us/en/documents/architecture-instruction-set-extensions-programming-reference.pdf
        let ext_cpuid_result = unsafe { __cpuid(7) };
        check_flag(ext_cpuid_result.edx as usize, 23)
    }

    pub fn has_avx512() -> bool {
        is_x86_feature_detected!("avx512f")
    }
}

// Inspired by https://github.com/RustCrypto/utils/blob/master/cpufeatures/src/aarch64.rs
// aarch64 doesn't have userspace feature detection built in, so we have to call
// into OS-specific functions to check for features.

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
mod aarch64 {
    pub fn has_neon_f16_support() -> bool {
        // Maybe we can assume it's there?
        true
    }
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
mod aarch64 {
    pub fn has_neon_f16_support() -> bool {
        // See: https://github.com/rust-lang/libc/blob/7ce81ca7aeb56aae7ca0237ef9353d58f3d7d2f1/src/unix/linux_like/linux/gnu/b64/aarch64/mod.rs#L533
        let flags = unsafe { libc::getauxval(libc::AT_HWCAP) };
        flags & libc::HWCAP_FPHP != 0
    }
}

#[cfg(all(target_arch = "aarch64", target_os = "windows"))]
mod aarch64 {
    pub fn has_neon_f16_support() -> bool {
        // https://github.com/lance-format/lance/issues/2411
        false
    }
}

#[cfg(target_arch = "loongarch64")]
mod loongarch64 {
    pub fn has_lsx_support() -> bool {
        // See: https://github.com/rust-lang/libc/blob/7ce81ca7aeb56aae7ca0237ef9353d58f3d7d2f1/src/unix/linux_like/linux/gnu/b64/loongarch64/mod.rs#L263
        let flags = unsafe { libc::getauxval(libc::AT_HWCAP) };
        flags & libc::HWCAP_LOONGARCH_LSX != 0
    }
    pub fn has_lasx_support() -> bool {
        // See: https://github.com/rust-lang/libc/blob/7ce81ca7aeb56aae7ca0237ef9353d58f3d7d2f1/src/unix/linux_like/linux/gnu/b64/loongarch64/mod.rs#L264
        let flags = unsafe { libc::getauxval(libc::AT_HWCAP) };
        flags & libc::HWCAP_LOONGARCH_LASX != 0
    }
}

#[cfg(all(target_arch = "aarch64", target_os = "android"))]
mod aarch64 {
    pub fn has_neon_f16_support() -> bool {
        false
    }
}
