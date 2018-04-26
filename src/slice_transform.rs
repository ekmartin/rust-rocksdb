// Copyright 2018 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::CString;
use std::slice;

use libc::{c_char, c_void, size_t};

use ffi;

/// A SliceTranform is a generic pluggable way of transforming one string
/// to another. Its primary use-case is in configuring rocksdb
/// to store prefix blooms by setting prefix_extractor in
/// ColumnFamilyOptions.
pub trait SliceTransformFns {
    // Extract a prefix from a specified key
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8];

    // Determine whether the specified key is compatible with the logic
    // specified in the Transform method. This method is invoked for every
    // key that is inserted into the db. If this method returns true,
    // then Transform is called to translate the key to its prefix and
    // that returned prefix is inserted into the bloom filter. If this
    // method returns false, then the call to Transform is skipped and
    // no prefix is inserted into the bloom filters.
    fn in_domain(&mut self, key: &[u8]) -> bool;

    // This is currently not used and remains here for backward compatibility.
    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

/// The result of calling rocksdb_slice_transform_create.
pub struct SliceTransform {
    pub inner: *mut ffi::rocksdb_slicetransform_t,
}

/// Passed on to rocksdb and used to retrieve the functions defined in SliceTransformFns.
#[repr(C)]
pub struct SliceTransformState {
    name: CString,
    transform: Box<SliceTransformFns>,
}

// NB we intentionally don't implement a Drop that passes
// through to rocksdb_slicetransform_destroy because
// this is currently only used (to my knowledge)
// by people passing it as a prefix extractor when
// opening a DB.

impl SliceTransform {
    pub fn create(
        name: &str,
        fns: Box<SliceTransformFns>,
    ) -> SliceTransform {
        let c_name = CString::new(name.as_bytes()).unwrap();
        let proxy = Box::into_raw(Box::new(SliceTransformState {
            name: c_name,
            transform: fns,
        }));

        let inner = unsafe {
            ffi::rocksdb_slicetransform_create(
                proxy as *mut c_void,
                Some(destructor),
                Some(transform),
                Some(in_domain),
                Some(in_range),
                Some(get_name),
            )
        };

        SliceTransform { inner }
    }

    pub fn create_fixed_prefix(len: size_t) -> SliceTransform {
        SliceTransform {
            inner: unsafe {
                ffi::rocksdb_slicetransform_create_fixed_prefix(len)
            },
        }
    }

    pub fn create_noop() -> SliceTransform {
        SliceTransform {
            inner: unsafe {
                ffi::rocksdb_slicetransform_create_noop()
            },
        }
    }
}

unsafe extern "C" fn get_name(transform: *mut c_void) -> *const c_char {
    (*(transform as *mut SliceTransformState)).name.as_ptr()
}

unsafe extern "C" fn destructor(transform: *mut c_void) {
    Box::from_raw(transform as *mut SliceTransformState);
}

unsafe extern "C" fn transform(
    transform: *mut c_void,
    key: *const c_char,
    key_len: size_t,
    dest_len: *mut size_t,
) -> *mut c_char {
    let transform = &mut *(transform as *mut SliceTransformState);
    let key = slice::from_raw_parts(key as *const u8, key_len);
    let prefix = transform.transform.transform(key);
    *dest_len = prefix.len() as size_t;
    prefix.as_ptr() as *mut c_char
}

unsafe extern "C" fn in_domain(transform: *mut c_void, key: *const c_char, key_len: size_t) -> u8 {
    let transform = &mut *(transform as *mut SliceTransformState);
    let key = slice::from_raw_parts(key as *const u8, key_len);
    transform.transform.in_domain(key) as u8
}

unsafe extern "C" fn in_range(transform: *mut c_void, key: *const c_char, key_len: size_t) -> u8 {
    let transform = &mut *(transform as *mut SliceTransformState);
    let key = slice::from_raw_parts(key as *const u8, key_len);
    transform.transform.in_range(key) as u8
}
