//! `rucio-emule` — eMule/Kad2 compatibility layer for Rucio.
//!
//! Provides:
//! - `ed2k`: ed2k:// link parsing and MD4-based hash computation.
//! - `kad`: Kad2 UDP packet codec, `nodes.dat` parser, and source search.
//! - `transfer`: minimal eMule TCP chunk download protocol.
//!
//! This crate has **zero coupling** to rucio-daemon internals; it exposes a
//! clean async API that the daemon integrates via the `emule-compat` feature.

pub mod ed2k;
pub mod kad;
pub mod progress;
pub mod transfer;

pub use ed2k::{Ed2kHash, Ed2kLink};
