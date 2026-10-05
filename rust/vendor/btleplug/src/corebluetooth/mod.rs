// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

pub mod adapter;
mod central_delegate;
mod ffi;
mod future;
mod internal;
pub mod manager;
pub mod peripheral;
mod utils;

mod tasks;

#[path = "../winrtble/ble/callback_boundary.rs"]
mod callback_boundary;

pub(crate) mod native_error;
