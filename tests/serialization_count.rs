use bincode::{de::Deserializer, options, Serializer};
use serde::Deserialize;
use sharify::Shared;

#[test]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create two slices backed by the same shared memory.
    let slice_a: Shared<[u64]> = Shared::new(&(0, 1_000_000))?;
    let slice_b = slice_a.clone();

    // Serialize both
    let mut bytes_a = Vec::new();
    let mut serializer = Serializer::new(&mut bytes_a, options());
    slice_a.into_serialized(&mut serializer).unwrap();
    let mut bytes_b = Vec::new();
    let mut serializer = Serializer::new(&mut bytes_b, options());
    slice_b.into_serialized(&mut serializer).unwrap();

    // Deserialize one of them TWICE and drop the `Shared`s
    {
        let mut deserializer = Deserializer::from_slice(&bytes_a, options());
        Shared::<[u64]>::deserialize(&mut deserializer)?;
        let mut deserializer = Deserializer::from_slice(&bytes_a, options());
        Shared::<[u64]>::deserialize(&mut deserializer)?;
    }

    // Because the two serializations have been cancelled out by the two
    // deserializations, the free memory has been dropped and `slice_b` cannot
    // be deserialized.
    let mut deserializer = Deserializer::from_slice(&bytes_b, options());
    assert!(Shared::<[u64]>::deserialize(&mut deserializer).is_err());

    Ok(())
}
