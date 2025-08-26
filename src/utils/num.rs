//! Numeric utilities: safe and centralized integer conversions.
//!
//! Guidelines
//! - Prefer fallible conversions (returning Option<T>) when a value out of range should stop the operation (e.g., parsing a length before slicing).
//! - Prefer saturating conversions when best-effort is acceptable and clamping is safer than panicking or truncating (e.g., converting a time span to u64 for metrics/logging).
//! - Prefer lossless widening with explicit helpers to keep call sites consistent and searchable.

#[inline]
#[must_use]
pub fn u64_to_usize(v: u64) -> Option<usize> {
    usize::try_from(v).ok()
}

#[inline]
#[must_use]
pub fn i32_to_usize(v: i32) -> Option<usize> {
    usize::try_from(v).ok()
}

#[inline]
#[must_use]
pub fn u16_to_usize(v: u16) -> usize {
    usize::from(v)
}

#[inline]
#[must_use]
pub fn u32_to_usize(v: u32) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

#[inline]
#[must_use]
pub fn i64_to_usize(v: i64) -> Option<usize> {
    usize::try_from(v).ok()
}

#[inline]
#[must_use]
pub fn f64_to_u64_saturating(v: f64) -> u64 {
    if !v.is_finite() {
        return 0;
    }
    if v <= 0.0 {
        0
    } else if v >= u64::MAX as f64 {
        u64::MAX
    } else {
        v as u64
    }
}

#[inline]
#[must_use]
pub fn usize_checked_add(a: usize, b: usize) -> Option<usize> {
    a.checked_add(b)
}

#[inline]
#[must_use]
pub fn usize_to_u64(v: usize) -> u64 {
    v as u64
}

#[inline]
#[must_use]
pub fn u128_to_u64_saturating(v: u128) -> u64 {
    if v > u128::from(u64::MAX) { u64::MAX } else { v as u64 }
}

#[inline]
#[must_use]
pub fn i64_to_u64_saturating_nonnegative(v: i64) -> u64 {
    if v <= 0 { 0 } else { v as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_to_usize_matches_std_try_from() {
        for &v in &[0u64, 1, 42, u32::MAX as u64, u64::from(u32::MAX) + 1] {
            assert_eq!(u64_to_usize(v), usize::try_from(v).ok());
        }
    }

    #[test]
    fn i32_to_usize_matches_std_try_from() {
        for &v in &[0i32, 1, 42, i32::MAX] {
            assert_eq!(i32_to_usize(v), usize::try_from(v).ok());
        }
        assert_eq!(i32_to_usize(-1), None);
    }

    #[test]
    fn u16_u32_helpers_behave() {
        assert_eq!(u16_to_usize(5), 5usize);
        let big: u32 = u32::MAX;
        let conv = u32_to_usize(big);
        if std::mem::size_of::<usize>() >= 4 {
            assert_eq!(conv, usize::try_from(big).unwrap_or(usize::MAX));
        } else {
            assert_eq!(conv, usize::MAX);
        }
    }

    #[test]
    fn f64_saturating_behaves() {
        assert_eq!(f64_to_u64_saturating(f64::NAN), 0);
        assert_eq!(f64_to_u64_saturating(-1.0), 0);
        assert_eq!(f64_to_u64_saturating(u64::MAX as f64 * 2.0), u64::MAX);
        assert_eq!(f64_to_u64_saturating(1234.56), 1234);
    }

    #[test]
    fn i64_to_u64_nonneg_saturating() {
        assert_eq!(i64_to_u64_saturating_nonnegative(-5), 0);
        assert_eq!(i64_to_u64_saturating_nonnegative(0), 0);
        assert_eq!(i64_to_u64_saturating_nonnegative(7), 7);
    }

    #[test]
    fn usize_checked_add_works() {
        assert_eq!(usize_checked_add(2, 3), Some(5));
        if let Some(max_minus_one) = usize::MAX.checked_sub(1) {
            assert_eq!(usize_checked_add(max_minus_one, 2), None);
        }
    }

    #[test]
    fn usize_to_u64_is_lossless() {
        let values = [0usize, 1, 42, 10_000, usize::BITS.min(63) as usize];
        for &v in &values {
            let w = usize_to_u64(v);
            assert_eq!(w as usize, v);
        }
    }

    #[test]
    fn u128_to_u64_saturating_edges() {
        assert_eq!(u128_to_u64_saturating(0), 0);
        assert_eq!(u128_to_u64_saturating(u64::MAX as u128), u64::MAX);
        assert_eq!(u128_to_u64_saturating(u64::MAX as u128 + 1), u64::MAX);
        assert_eq!(u128_to_u64_saturating(u128::MAX), u64::MAX);
    }
}
