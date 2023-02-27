// Copyright 2021-2023 Protocol Labs
// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

// disable this lint because it can actually cause performance regressions, and usually leads to
// hard to read code.
#![allow(clippy::comparison_chain)]

use std::collections::BTreeSet;
use std::ops::Range;

use fvm_ipld_amt::Amt;
use fvm_ipld_bitfield::iter::Ranges;
use fvm_ipld_bitfield::{bitfield, BitField};
use fvm_ipld_blockstore::Blockstore;

#[derive(Debug, Clone)]
pub struct Meta {
    pub(crate) first_value: u64,
    // TODO: eliminate recordin of last_value (can be derived from first_value of next meta)
    pub(crate) last_value: u64,
    pub(crate) run_count: usize,
}

#[derive(Debug)]
pub struct BigField<'a, BS> {
    pub(crate) meta: Vec<Meta>, // (key, Range) ordered by range, ranges are non-overalpping
    pub(crate) fields: Amt<BitField, &'a BS>, // cid -> Hamt<BS, Cid, u64, Identity> TODO: inline this into state
    pub(crate) _store: &'a BS,

    _set: BTreeSet<u64>,
    _unset: BTreeSet<u64>,
}

impl<'a, BS> BigField<'a, BS>
where
    BS: Blockstore,
{
    pub fn new(store: &'a BS) -> Self {
        Self {
            meta: vec![Meta {
                first_value: 0,
                last_value: u64::MAX,
                run_count: 0,
            }],
            fields: Amt::new_with_bit_width(store, 8),
            _store: store,
            _set: Default::default(),
            _unset: Default::default(),
        }
    }

    pub fn set(&mut self, index: u64) {
        let meta_index = self
            .meta
            .binary_search_by(|meta| {
                if meta.first_value <= index && index < meta.last_value {
                    // TODO: see if index <= meta.last_value could work
                    std::cmp::Ordering::Equal
                } else if index < meta.first_value {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .unwrap();

        let mut meta = self.meta.get_mut(meta_index).unwrap();

        let mut bitfield = match self.fields.get(meta.first_value).unwrap() {
            Some(b) => b.clone(),
            None => BitField::new(),
        };

        const SPLIT_FACTOR: usize = 4;
        if meta.run_count >= SPLIT_FACTOR {
            println!("Splitting");
            // FIXME: searching run count here could fail if bitfield has many runs in the BTree sets
            // TODO tune this number
            // split the range in half // TODO: there may be a more optimal splitting depending on the distribution of runs
            let bottom_ranges = bitfield.ranges().take(SPLIT_FACTOR / 2);
            let top_ranges = bitfield.ranges().skip(SPLIT_FACTOR / 2);
            let mut bottom_bitfield = BitField::from_ranges(Ranges::new(bottom_ranges));
            let mut top_bitfield = BitField::from_ranges(Ranges::new(top_ranges));
            let bottom_meta = Meta {
                first_value: meta.first_value,
                run_count: bottom_bitfield.ranges().count(),
                last_value: top_bitfield.first().unwrap(),
            };
            let top_meta = Meta {
                first_value: bottom_meta.last_value,
                last_value: meta.last_value,
                run_count: top_bitfield.ranges().count(),
            };

            // create a bitfield mask completely filled with 1s up till bottom_meta.last_value
            let mask = BitField::from_ranges(Ranges::new(vec![Range {
                start: bottom_meta.first_value,
                end: bottom_meta.last_value,
            }]));
            top_bitfield = top_bitfield.cut(&mask);

            if index >= top_meta.first_value {
                top_bitfield.set(index - top_meta.first_value);
            } else {
                bottom_bitfield.set(index - bottom_meta.first_value);
            }

            self.fields
                .set(bottom_meta.first_value, bottom_bitfield)
                .unwrap();
            self.fields.set(top_meta.first_value, top_bitfield).unwrap();

            self.meta.splice(
                meta_index..meta_index + 1,
                [bottom_meta, top_meta].iter().cloned(),
            );
        } else {
            bitfield.set(index as u64 - meta.first_value);
            meta.run_count = bitfield.ranges().count();
            self.fields.set(meta.first_value, bitfield).unwrap();
        }
    }

    pub fn get(&self, index: u64) -> bool {
        let meta_index = self
            .meta
            .binary_search_by(|meta| {
                if meta.first_value <= index && index < meta.last_value {
                    // TODO: see if index <= meta.last_value could work
                    std::cmp::Ordering::Equal
                } else if index < meta.first_value {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .unwrap();

        let meta = self.meta.get(meta_index).unwrap();

        let bitfield = self.fields.get(meta.first_value).unwrap().unwrap().clone();
        bitfield.get(index as u64 - meta.first_value)
    }

    fn print_bitfields(&self) {
        self.fields
            .for_each(|i, b| {
                println!("{} {:?}", i, b);
                Ok(())
            })
            .unwrap();
    }

    // pub fn try_set(&mut self, bit: u64) -> Result<(), OutOfRangeError> {
    // if bit == u64::MAX {
    // return Err(OutOfRangeError);
    // }
    // self.unset.remove(&bit);
    // self.set.insert(bit);
    // Ok(())
    // }
}

#[cfg(test)]
mod test {
    use fvm_ipld_bitfield::BitField;
    use fvm_ipld_blockstore::tracking::TrackingBlockstore;
    use fvm_ipld_blockstore::MemoryBlockstore;
    use fvm_ipld_encoding::CborStore;
    use fvm_ipld_hamt::{Hamt, Identity};

    use crate::BigField;

    #[test]
    pub fn bigfield() {
        let store = MemoryBlockstore::default();
        let store = TrackingBlockstore::new(store);
        let mut bigfield = BigField::new(&store);

        bigfield.set(1);
        bigfield.set(3);
        bigfield.set(5);
        bigfield.set(7);

        bigfield.print_bitfields();

        bigfield.set(9);
        bigfield.print_bitfields();

        assert!(bigfield.get(1));
        assert!(!bigfield.get(2));
        assert!(!bigfield.get(8));
        assert!(bigfield.get(9));
    }

    #[test]
    pub fn benchmark_bitfield() {
        {
            let store = MemoryBlockstore::default();
            let store = TrackingBlockstore::new(store);
            let mut bitfield = BitField::new();
            store
                .put_cbor(&bitfield, cid::multihash::Code::Blake2b256)
                .unwrap();

            println!("Write empty bitfield {:?}", store.stats);
            store.stats.take();

            bitfield.set(u64::MAX / 2);
            store
                .put_cbor(&bitfield, cid::multihash::Code::Blake2b256)
                .unwrap();

            println!("Write bitfield with one entry in middle {:?}", store.stats);
            store.stats.take();

            bitfield.set((u64::MAX / 4) * 3);
            store
                .put_cbor(&bitfield, cid::multihash::Code::Blake2b256)
                .unwrap();

            println!(
                "Write bitfield with one more entry in middle {:?}",
                store.stats
            );
            store.stats.take();

            for i in 0..100000 {
                bitfield.set(i * (2 ^ 50));
            }
            store
                .put_cbor(&bitfield, cid::multihash::Code::Blake2b256)
                .unwrap();

            println!(
                "Write bitfield with one more entry in middle {:?}",
                store.stats
            );
            store.stats.take();
        }
    }

    #[test]
    pub fn test_hamt_iteration_identity() {
        let store = MemoryBlockstore::default();
        let mut hamt = Hamt::<MemoryBlockstore, u64, u64, Identity>::new(store);
        // set ten incrementing values
        for i in 0..10 {
            hamt.set(i, i).unwrap();
        }

        hamt.for_each(|i, _v| {
            println!("{}", i);
            Ok(())
        })
        .unwrap();
    }
}
