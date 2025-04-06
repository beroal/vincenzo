//! Converting various types of ranges. Other range utilities.
//! [`Int`] is a trait for integer numbers.

use std::num::Wrapping;
use std::ops::{
    Bound::{self, Included, Excluded, Unbounded},
    RangeBounds,
    Add,
    Sub,
    Range,
    RangeInclusive,
};
use num::{traits::{bounds::*, identities::{Zero, One}}, BigInt, BigUint};
use thiserror::Error;



/// Integers.
/// `N: Int` iff for all `x` in `N`:
///
/// - the least `y` such that `y > x` (if it exists) is `x+1`, and
/// - the greatest `y` such that `y < x` (if it exists) is `x-1`.
pub trait Int {}

impl Int for i8 {}
impl Int for i16 {}
impl Int for i32 {}
impl Int for i64 {}
impl Int for i128 {}
impl Int for isize {}
impl Int for u8 {}
impl Int for u16 {}
impl Int for u32 {}
impl Int for u64 {}
impl Int for u128 {}
impl Int for usize {}

impl Int for BigInt {}
impl Int for BigUint {}

impl<N: Int> Int for Wrapping<N> {}



/// Returns `b` such that for all `x`, `x` is greater than `a` iff `x >= b`.
/// If `a == Excluded(N::max_value())`, the result is undefined.
pub fn included_start_bound<N>(a: Bound<&N>) -> N
where N: Clone + One + Add<Output = N> + Int + LowerBounded
{
    match a {
        Included(a) => a.clone(),
        Excluded(a) => a.clone() + N::one(),
        Unbounded => N::min_value(),
    }
}

/// Returns `Some(b)` such that for all `x`,
/// `x` is greater than `a` iff `x > b`.
/// If `a == Included(N::min_value())` or `a == Unbounded`,
/// the result is undefined.
pub fn excluded_start_bound<N>(a: Bound<&N>) -> N
where N: Clone + Default + One + Sub<Output = N> + Int
{
    match a {
        Included(a) => a.clone() - N::one(),
        Excluded(a) => a.clone(),
        Unbounded => Default::default(),
    }
}

/// Returns `b` such that for all `x`, `x` is less than `a` iff `x <= b`.
/// If `a == Excluded(N::min_value())`, the result is undefined.
pub fn included_end_bound<N>(a: Bound<&N>) -> N
where N: Clone + One + Sub<Output = N> + UpperBounded + Int
{
    match a {
        Included(a) => a.clone(),
        Excluded(a) => a.clone() - N::one(),
        Unbounded => N::max_value(),
    }
}

/// Returns `Some(b)` such that for all `x`, `x` is less than `a` iff `x < b`.
/// If `a == Included(N::max_value())` or `a == Unbounded`,
/// the result is undefined.
pub fn excluded_end_bound<N>(a: Bound<&N>) -> N
where N: Clone + Default + One + Add<Output = N> + Int
{
    match a {
        Included(a) => a.clone() + N::one(),
        Excluded(a) => a.clone(),
        Unbounded => Default::default(),
    }
}

pub trait TryFromRange<R>: Sized {
    type Error;

    /// If it returns `Ok(r)`, then `r` represents the same set as the argument.
    fn try_from_range(range_bounds: R) -> Result<Self, Self::Error>;
}

#[derive(Debug, Error)]
pub enum TryFromRangeError{
    #[error("the start range bound is too big")]
    StartBig,

    #[error("the end range bound is too small")]
    EndSmall,

    #[error("the end range bound is too big")]
    EndBig,
}

impl<N, R: RangeBounds<N>> TryFromRange<R> for RangeInclusive<N>
where
    N: Clone + Default + One + Add<Output = N> + Sub<Output = N>
        + Int + Bounded + PartialEq
{
    type Error = TryFromRangeError;

    fn try_from_range(range_bounds: R) -> Result<Self, Self::Error> {
        let start = if range_bounds.start_bound() == Excluded(&N::max_value()) {
             Err(TryFromRangeError::StartBig)
        } else {
            Ok(included_start_bound(range_bounds.start_bound()))
        }?;
        let end = if range_bounds.end_bound() == Excluded(&N::min_value()) {
            Err(TryFromRangeError::EndSmall)
        } else {
            Ok(included_end_bound(range_bounds.end_bound()))
        }?;
        Ok(start ..= end)
    }
}

impl<N, R: RangeBounds<N>> TryFromRange<R> for Range<N>
where N: Clone + Default + One + Add<Output = N> + Int + Bounded + PartialEq
{
    type Error = TryFromRangeError;

    fn try_from_range(range_bounds: R) -> Result<Self, Self::Error> {
        let start = if range_bounds.start_bound() == Excluded(&N::max_value()) {
            Err(TryFromRangeError::StartBig)
        } else {
            Ok(included_start_bound(range_bounds.start_bound()))
        }?;
        let end_bound = range_bounds.end_bound();
        let is_end_max = end_bound == Unbounded
            || end_bound == Included(&N::max_value());
        let end = if is_end_max {
            Err(TryFromRangeError::EndBig)
        } else {
            Ok(excluded_end_bound(range_bounds.end_bound()))
        }?;
        Ok(start .. end)
    }
}

/// The length of a `range`.
pub fn range_len<N: Zero + Sub<Output = N> + PartialOrd>(range: Range<N>) -> N {
    if range.is_empty() { N::zero() } else { range.end - range.start }
}
