use crate::clone_in::CloneIn;

use std::{
    alloc::{Allocator, Layout},
    fmt::{Debug, Display, Error, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{size_of, transmute, zeroed, MaybeUninit},
    ptr::copy_nonoverlapping,
    slice::{from_raw_parts, from_raw_parts_mut},
    str::{from_utf8_unchecked, from_utf8_unchecked_mut},
};

const INLINE_CAP: usize = 15;

const MARKER_LEN_MASK: u8 = 0b0111_1111;
const MARKER_DISC_MASK: u8 = !MARKER_LEN_MASK;

pub struct Unigram<A: Allocator> {
    raw: MaybeUninit<Repr<A>>,
}

#[repr(packed)]
struct Repr<A: Allocator> {
    data: [u8; INLINE_CAP],
    marker: Marker,

    alloc: PhantomData<A>,
}

#[derive(PartialEq, Eq)]
struct Marker(u8);

impl Marker {
    fn new_inline(len: usize) -> Self {
        Self(len as u8)
    }

    fn new_boxed(len: usize) -> Self {
        Self((len as u8) | MARKER_DISC_MASK)
    }

    fn len(&self) -> usize {
        (self.0 & MARKER_LEN_MASK) as usize
    }

    fn is_inline(&self) -> bool {
        self.0 & MARKER_DISC_MASK == 0
    }
}

impl<A: Allocator> Unigram<A> {
    unsafe fn inner(&self) -> &Repr<A> {
        &*self.raw.as_ptr()
    }

    unsafe fn inner_mut(&mut self) -> &mut Repr<A> {
        &mut *self.raw.as_mut_ptr()
    }

    pub fn len(&self) -> usize {
        unsafe { self.inner().marker.len() }
    }

    pub fn is_inline(&self) -> bool {
        unsafe { self.inner().marker.is_inline() }
    }

    unsafe fn data_ptr(&self) -> *const u8 {
        match self.is_inline() {
            true => &self.inner().data as *const u8,
            false => {
                let ptr: *const *const u8 = transmute(&self.inner().data);
                *ptr
            }
        }
    }

    unsafe fn data_ptr_mut(&mut self) -> *mut u8 {
        match self.is_inline() {
            true => &mut self.inner_mut().data as *mut u8,
            false => {
                let ptr: *mut *mut u8 = transmute(&self.inner_mut().data);
                *ptr
            }
        }
    }

    pub fn from_slice_in(slice: &str, alloc: A) -> Self {
        if slice.len() > INLINE_CAP {
            unsafe { Self::from_slice_in_boxed(slice, alloc) }
        } else {
            unsafe { Self::from_slice_inline(slice) }
        }
    }

    unsafe fn from_slice_in_boxed(slice: &str, alloc: A) -> Self {
        let mut out = Self {
            raw: MaybeUninit::uninit(),
        };

        let layout = Layout::from_size_align_unchecked(slice.len(), 16);
        let data = alloc.allocate(layout.pad_to_align()).unwrap().as_mut_ptr();

        copy_nonoverlapping(slice.as_ptr(), data, slice.len());

        let out_data_ptr: *mut *mut u8 = transmute(&out.inner_mut().data);
        out_data_ptr.write(data);

        out.inner_mut().marker = Marker::new_boxed(slice.len());
        out
    }

    unsafe fn from_slice_inline(slice: &str) -> Self {
        let mut out = Self {
            raw: MaybeUninit::new(zeroed()),
        };

        copy_nonoverlapping(
            slice.as_ptr(),
            out.inner_mut().data.as_mut_ptr(),
            slice.len(),
        );

        out.inner_mut().marker = Marker::new_inline(slice.len());
        out
    }

    pub fn as_str(&self) -> &str {
        unsafe {
            let data = from_raw_parts(self.data_ptr(), self.len());
            from_utf8_unchecked(data)
        }
    }

    pub fn as_mut_str(&mut self) -> &mut str {
        unsafe {
            let data = from_raw_parts_mut(self.data_ptr_mut(), self.len());
            from_utf8_unchecked_mut(data)
        }
    }
}

impl<A: Allocator> CloneIn<A> for Unigram<A> {
    fn clone_in(&self, alloc: A) -> Self {
        if self.is_inline() {
            let mut out = Self {
                raw: MaybeUninit::uninit(),
            };

            unsafe {
                let src = self.raw.as_ptr();
                let dst = out.raw.as_mut_ptr();

                copy_nonoverlapping(src, dst, size_of::<Self>());
            }

            out
        } else {
            unsafe { Self::from_slice_in_boxed(self.as_str(), alloc) }
        }
    }
}

impl<A: Allocator> Drop for Unigram<A> {
    fn drop(&mut self) {}
}

impl<A: Allocator> AsRef<str> for Unigram<A> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<A: Allocator> AsMut<str> for Unigram<A> {
    fn as_mut(&mut self) -> &mut str {
        self.as_mut_str()
    }
}

impl<A: Allocator> Hash for Unigram<A> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        if self.is_inline() {
            hasher.write_u128(unsafe { *(self.raw.as_ptr() as *const u128) });
        } else {
            self.as_str().hash(hasher);
        }
    }
}

impl<A: Allocator> Debug for Unigram<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        Debug::fmt(self.as_str(), f)
    }
}

impl<A: Allocator> Display for Unigram<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        Display::fmt(self.as_str(), f)
    }
}

impl<A: Allocator> From<Unigram<A>> for String {
    fn from(unigram: Unigram<A>) -> Self {
        unigram.as_str().into()
    }
}

impl<A: Allocator> From<&Unigram<A>> for String {
    fn from(unigram: &Unigram<A>) -> Self {
        unigram.as_str().into()
    }
}

impl<A: Allocator> PartialEq<Unigram<A>> for Unigram<A> {
    fn eq(&self, other: &Unigram<A>) -> bool {
        unsafe {
            if self.inner().marker != other.inner().marker {
                return false;
            }
        }

        if self.is_inline() {
            let v1 = unsafe { *(self.raw.as_ptr() as *const u128) };
            let v2 = unsafe { *(other.raw.as_ptr() as *const u128) };

            return v1 == v2;
        }

        self.as_str() == other.as_str()
    }
}

impl<A: Allocator> Eq for Unigram<A> {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        alloc::{AllocError, Global},
        ptr::NonNull,
    };

    #[derive(Clone, Copy)]
    struct TestAllocator();

    unsafe impl Allocator for TestAllocator {
        fn allocate(&self, _: Layout) -> Result<NonNull<[u8]>, AllocError> {
            panic!("attempted to allocate")
        }

        unsafe fn deallocate(&self, _: NonNull<u8>, _: Layout) {
            panic!("attempted to deallocate")
        }
    }

    const TEST_STRS: [&'static str; 6] = [
        "",
        "T",
        "The quick",
        "The quick brown",
        "The quick brown ",
        "The quick brown fox jumps over the lazy dog",
    ];

    #[test]
    fn test_size() {
        assert_eq!(size_of::<Unigram<Global>>(), 16);
        assert_eq!(size_of::<Unigram<Global>>(), INLINE_CAP + 1);

        assert_eq!(
            size_of::<Unigram<Global>>(),
            size_of::<Unigram<TestAllocator>>()
        );
    }

    #[test]
    fn test_length() {
        for s in TEST_STRS {
            let u = Unigram::from_slice_in(s, Global::default());
            assert_eq!(u.len(), s.len());
        }
    }

    #[test]
    fn test_inline() {
        for s in TEST_STRS {
            let u = Unigram::from_slice_in(s, Global::default());
            assert_eq!(u.is_inline(), s.len() <= INLINE_CAP);
        }
    }

    #[test]
    fn test_inline_alloc() {
        let a = TestAllocator();
        for s in TEST_STRS.iter().filter(|s| s.len() <= INLINE_CAP) {
            let u = Unigram::from_slice_in(s, a);
            assert_eq!(u.is_inline(), true);
        }
    }

    #[test]
    #[should_panic(expected = "attempted to allocate")]
    fn test_box_alloc() {
        let a = TestAllocator();
        let u = Unigram::from_slice_in(TEST_STRS[4], a);

        assert_eq!(u.is_inline(), false);
    }

    #[test]
    fn test_eq() {
        for s in TEST_STRS {
            let u1 = Unigram::from_slice_in(s, Global::default());
            let u2 = Unigram::from_slice_in(s, Global::default());

            assert!(u1 == u2);
            assert!(!(u1 != u2));
        }
    }

    #[test]
    fn test_neq() {
        for (s1, s2) in TEST_STRS.iter().zip(&TEST_STRS[1..]) {
            let u1 = Unigram::from_slice_in(s1, Global::default());
            let u2 = Unigram::from_slice_in(s2, Global::default());

            assert!(u1 != u2);
            assert!(!(u1 == u2));
        }
    }

    #[test]
    fn test_eq_str() {
        for s in TEST_STRS {
            let u = Unigram::from_slice_in(s, Global::default());

            assert!(u.as_str() == s);
            assert!(s == u.as_str());
        }
    }
}
