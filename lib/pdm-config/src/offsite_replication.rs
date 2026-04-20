use anyhow::{Context, Error};

use proxmox_product_config::{open_api_lockfile, replace_config, ApiLockGuard};

use pdm_api_types::{ConfigDigest, OffsiteReplicationConfig};

use pdm_buildcfg::configdir;

const OFFSITE_REPLICATION_CFG_FILENAME: &str = configdir!("/offsite-replication.cfg");
const OFFSITE_REPLICATION_CFG_LOCKFILE: &str = configdir!("/.offsite-replication.lock");

/// Get the `offsite-replication.cfg` config file contents.
pub fn config() -> Result<(OffsiteReplicationConfig, ConfigDigest), Error> {
    let content = proxmox_sys::fs::file_read_optional_string(OFFSITE_REPLICATION_CFG_FILENAME)?
        .unwrap_or_default();

    let digest = openssl::sha::sha256(content.as_bytes());

    let config = if content.trim().is_empty() {
        OffsiteReplicationConfig::default()
    } else {
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse '{}'", OFFSITE_REPLICATION_CFG_FILENAME))?
    };

    Ok((config, digest.into()))
}

/// Get exclusive lock.
pub fn lock_config() -> Result<ApiLockGuard, Error> {
    open_api_lockfile(OFFSITE_REPLICATION_CFG_LOCKFILE, None, true)
}

/// Save `offsite-replication.cfg`.
pub fn save_config(config: &OffsiteReplicationConfig) -> Result<(), Error> {
    let raw = serde_json::to_vec_pretty(config)?;
    replace_config(OFFSITE_REPLICATION_CFG_FILENAME, &raw)?;
    Ok(())
}
