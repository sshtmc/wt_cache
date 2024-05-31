use std::path::PathBuf;

use wt_cache::WriteThroughCache;

fn main() -> std::io::Result<()> {
    let mut cache = WriteThroughCache::new(&PathBuf::from("cache.dat"), None, None)?;

    let address = 0;
    let data = vec![1; 1024]; // Write 1024 bytes

    cache.write(address, &data)?;
    let read_data = cache.read(address, 1024)?;

    assert_eq!(data, read_data);

    Ok(())
}
