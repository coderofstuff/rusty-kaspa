use kaspa_database::{
    prelude::DB,
    prelude::{CachePolicy, StoreError, StoreResult},
    prelude::{CachedDbAccess, DbKey, DirectDbWriter},
    registry::DatabaseStorePrefixes,
};
use kaspa_utils::mem_size::MemSizeEstimator;
use kaspa_utils::networking::IpAddress;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;
use std::{error::Error, fmt::Display, sync::Arc};

use super::AddressKey;
use crate::NetAddress;

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry {
    pub connection_failed_count: u64,
    pub address: NetAddress,
}

impl MemSizeEstimator for Entry {}

#[derive(Clone, Serialize, Deserialize)]
struct LegacyNetAddress {
    ip: IpAddress,
    port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
struct LegacyEntry {
    connection_failed_count: u64,
    address: LegacyNetAddress,
}

impl From<LegacyEntry> for Entry {
    fn from(value: LegacyEntry) -> Self {
        Self { connection_failed_count: value.connection_failed_count, address: NetAddress::new(value.address.ip, value.address.port) }
    }
}

pub struct LoadedEntry {
    pub key: AddressKey,
    pub entry: Entry,
    pub migrated_legacy_format: bool,
}

pub trait AddressesStoreReader {
    #[allow(dead_code)]
    fn get(&self, key: AddressKey) -> Result<Entry, StoreError>;
}

pub trait AddressesStore: AddressesStoreReader {
    fn set(&mut self, key: AddressKey, entry: Entry) -> StoreResult<()>;
    #[allow(dead_code)]
    fn set_failed_count(&mut self, key: AddressKey, connection_failed_count: u64) -> StoreResult<()>;
    fn remove(&mut self, key: AddressKey) -> StoreResult<()>;
}

const IPV6_LEN: usize = 16;
const PORT_LEN: usize = 2;
pub const ADDRESS_KEY_SIZE: usize = IPV6_LEN + PORT_LEN;

// TODO: This pattern is used a lot. Think of some macro or any other way to generalize it.
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone)]
struct DbAddressKey([u8; ADDRESS_KEY_SIZE]);

impl AsRef<[u8]> for DbAddressKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Display for DbAddressKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ip_port: AddressKey = (*self).into();
        write!(f, "{}:{}", ip_port.0, ip_port.1)
    }
}

impl From<AddressKey> for DbAddressKey {
    fn from(key: AddressKey) -> Self {
        let mut bytes = [0; ADDRESS_KEY_SIZE];
        bytes[..IPV6_LEN].copy_from_slice(&key.0.octets());
        bytes[IPV6_LEN..].copy_from_slice(&key.1.to_le_bytes());
        Self(bytes)
    }
}

impl From<DbAddressKey> for AddressKey {
    fn from(k: DbAddressKey) -> Self {
        let ip_byte_array: [u8; 16] = k.0[..IPV6_LEN].try_into().unwrap();
        let ip: Ipv6Addr = ip_byte_array.into();
        let port_byte_array: [u8; 2] = k.0[IPV6_LEN..].try_into().unwrap();
        let port = u16::from_le_bytes(port_byte_array);
        AddressKey::new(ip, port)
    }
}

#[derive(Clone)]
pub struct DbAddressesStore {
    db: Arc<DB>,
    access: CachedDbAccess<DbAddressKey, Entry>,
}

impl DbAddressesStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, DatabaseStorePrefixes::Addresses.into()) }
    }

    pub fn iterator_with_legacy_migration(&self) -> impl Iterator<Item = Result<LoadedEntry, Box<dyn Error>>> + '_ {
        let prefix: Vec<u8> = DatabaseStorePrefixes::Addresses.into();
        let prefix_key = DbKey::prefix_only(&prefix);
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_range(rocksdb::PrefixRange(prefix_key.as_ref()));
        self.db.iterator_opt(IteratorMode::From(prefix_key.as_ref(), Direction::Forward), read_opts).map(
            move |iter_result| -> Result<LoadedEntry, Box<dyn Error>> {
                let (key, data_bytes) = match iter_result {
                    Ok(data) => data,
                    Err(err) => return Err(err.into()),
                };
                let key_slice = &key[prefix_key.prefix_len()..];
                let address_key_slice: [u8; ADDRESS_KEY_SIZE] =
                    key_slice.try_into().map_err(|err| -> Box<dyn Error> { Box::new(err) })?;
                let addr_key = DbAddressKey(address_key_slice);
                let key: AddressKey = addr_key.into();
                match bincode::deserialize::<Entry>(&data_bytes) {
                    Ok(entry) => Ok(LoadedEntry { key, entry, migrated_legacy_format: false }),
                    Err(primary_err) => match bincode::deserialize::<LegacyEntry>(&data_bytes) {
                        Ok(legacy_entry) => Ok(LoadedEntry { key, entry: legacy_entry.into(), migrated_legacy_format: true }),
                        Err(_) => Err(primary_err.into()),
                    },
                }
            },
        )
    }

    pub fn clear(&mut self) -> StoreResult<()> {
        self.access.delete_all(DirectDbWriter::new(&self.db))
    }
}

impl AddressesStoreReader for DbAddressesStore {
    fn get(&self, key: AddressKey) -> Result<Entry, StoreError> {
        self.access.read(key.into())
    }
}

impl AddressesStore for DbAddressesStore {
    fn set(&mut self, key: AddressKey, entry: Entry) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), key.into(), entry)
    }

    fn remove(&mut self, key: AddressKey) -> StoreResult<()> {
        self.access.delete(DirectDbWriter::new(&self.db), key.into())
    }

    fn set_failed_count(&mut self, key: AddressKey, connection_failed_count: u64) -> StoreResult<()> {
        let entry = self.get(key)?;
        self.set(key, Entry { connection_failed_count, address: entry.address })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use kaspa_database::{create_temp_db, prelude::ConnBuilder};
    use kaspa_utils::networking::IpAddress;

    use super::*;

    #[test]
    fn iterator_migrates_legacy_entries() {
        let (db_lifetime, db) = create_temp_db!(ConnBuilder::default().with_files_limit(16));
        let address = NetAddress::new(IpAddress::from_str("1.2.3.4").unwrap(), 16111);
        let key = AddressKey::from(&address);
        let db_key = DbKey::new(&Vec::<u8>::from(DatabaseStorePrefixes::Addresses), DbAddressKey::from(key));
        let legacy_entry =
            LegacyEntry { connection_failed_count: 7, address: LegacyNetAddress { ip: address.ip, port: address.port } };
        let legacy_bytes = bincode::serialize(&legacy_entry).unwrap();
        db.put(db_key, legacy_bytes).unwrap();

        let store = DbAddressesStore::new(db.clone(), CachePolicy::Empty);
        let loaded = store.iterator_with_legacy_migration().next().unwrap().unwrap();

        assert!(loaded.migrated_legacy_format);
        assert!(loaded.key == key);
        assert_eq!(loaded.entry.connection_failed_count, 7);
        assert_eq!(loaded.entry.address.ip, address.ip);
        assert_eq!(loaded.entry.address.port, address.port);
        drop(store);
        drop(db);
        drop(db_lifetime);
    }
}
