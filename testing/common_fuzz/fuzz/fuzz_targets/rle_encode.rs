#![no_main]
use libfuzzer_sys::fuzz_target;
use fvm_ipld_bitfield::BitField;
use arbitrary::Arbitrary;

#[derive(Debug, Arbitrary)]
enum Operation {
    Set(u64),
    Unset(u64),
}

fuzz_target!(|data: (BitField, Vec<Operation>)| {
    let (mut bf, ops) = data;

    for op in ops {
        match op {
            Operation::Set(x) => { _ = bf.try_set(x);}
            Operation::Unset(x) => {bf.unset(x);}
        };
    }

    let bf_bytes = bf.to_bytes();
    let bf2 = BitField::from_bytes(&bf_bytes).unwrap();
    assert_eq!(bf, bf2);

    let bf2_bytes = bf.to_bytes();
    assert_eq!(bf_bytes, bf2_bytes);
});
