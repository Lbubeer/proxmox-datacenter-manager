use serde::{Deserialize, Serialize};

use anyhow::{bail, Error};

use proxmox_schema::{api, const_regex, ApiStringFormat, IntegerSchema, Schema, StringSchema};

use crate::resource::GuestType;
use crate::PROXMOX_SAFE_ID_FORMAT;

pub const OFFSITE_REPLICATION_ID_SCHEMA: Schema = StringSchema::new("Off-site replication job ID.")
    .format(&PROXMOX_SAFE_ID_FORMAT)
    .min_length(2)
    .max_length(64)
    .schema();

const_regex! {
    /// Conservative ZFS dataset path accepted for target-side replication roots.
    pub OFFSITE_REPLICATION_TARGET_DATASET_REGEX = r"^[A-Za-z0-9][A-Za-z0-9_.:-]*(/[A-Za-z0-9][A-Za-z0-9_.:-]*)*$";

    /// SSH login name used in local SSH/scp command construction.
    pub OFFSITE_REPLICATION_SSH_USER_REGEX = r"^[A-Za-z_][A-Za-z0-9_.-]{0,31}\$?$";

    /// Absolute local private key path without shell metacharacters or whitespace.
    pub OFFSITE_REPLICATION_SSH_KEY_PATH_REGEX = r"^/(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+$";

    /// Source snapshot identifier as emitted by pve-zsync/ZFS.
    pub OFFSITE_REPLICATION_SNAPSHOT_REGEX = r"^[A-Za-z0-9][A-Za-z0-9_.:-]*(/[A-Za-z0-9][A-Za-z0-9_.:-]*)*@[A-Za-z0-9][A-Za-z0-9_.:-]*$";

    /// Conservative guest name override for recovered VMs.
    pub OFFSITE_REPLICATION_RECOVERED_NAME_REGEX = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$";
}

pub const OFFSITE_REPLICATION_TARGET_DATASET_FORMAT: ApiStringFormat =
    ApiStringFormat::VerifyFn(verify_offsite_target_dataset);

pub const OFFSITE_REPLICATION_SSH_USER_FORMAT: ApiStringFormat =
    ApiStringFormat::Pattern(&OFFSITE_REPLICATION_SSH_USER_REGEX);

pub const OFFSITE_REPLICATION_SSH_KEY_PATH_FORMAT: ApiStringFormat =
    ApiStringFormat::VerifyFn(verify_offsite_ssh_key_path);

pub const OFFSITE_REPLICATION_SNAPSHOT_FORMAT: ApiStringFormat =
    ApiStringFormat::VerifyFn(verify_offsite_snapshot);

pub const OFFSITE_REPLICATION_RECOVERED_NAME_FORMAT: ApiStringFormat =
    ApiStringFormat::Pattern(&OFFSITE_REPLICATION_RECOVERED_NAME_REGEX);

pub const OFFSITE_REPLICATION_TARGET_DATASET_SCHEMA: Schema =
    StringSchema::new("Target ZFS dataset used as off-site replication root.")
        .format(&OFFSITE_REPLICATION_TARGET_DATASET_FORMAT)
        .min_length(1)
        .max_length(255)
        .schema();

pub const OFFSITE_REPLICATION_SSH_USER_SCHEMA: Schema =
    StringSchema::new("SSH login user for off-site replication.")
        .format(&OFFSITE_REPLICATION_SSH_USER_FORMAT)
        .min_length(1)
        .max_length(32)
        .schema();

pub const OFFSITE_REPLICATION_SSH_KEY_PATH_SCHEMA: Schema =
    StringSchema::new("Absolute local SSH private key path for off-site replication.")
        .format(&OFFSITE_REPLICATION_SSH_KEY_PATH_FORMAT)
        .min_length(2)
        .max_length(255)
        .schema();

pub const OFFSITE_REPLICATION_SNAPSHOT_SCHEMA: Schema =
    StringSchema::new("Source snapshot identifier for an off-site recovery point.")
        .format(&OFFSITE_REPLICATION_SNAPSHOT_FORMAT)
        .min_length(3)
        .max_length(512)
        .schema();

pub const OFFSITE_REPLICATION_RECOVERED_NAME_SCHEMA: Schema =
    StringSchema::new("Recovered guest name.")
        .format(&OFFSITE_REPLICATION_RECOVERED_NAME_FORMAT)
        .min_length(1)
        .max_length(63)
        .schema();

pub const OFFSITE_REPLICATION_SCHEDULE_SCHEMA: Schema =
    StringSchema::new("Replication schedule in systemd calendar-event format.")
        .min_length(1)
        .max_length(64)
        .schema();

pub const OFFSITE_REPLICATION_MAXSNAP_SCHEMA: Schema =
    IntegerSchema::new("Maximum snapshots kept.")
        .minimum(1)
        .maximum(64)
        .schema();

pub const OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA: Schema =
    IntegerSchema::new("Maximum replication runs kept in job history.")
        .minimum(1)
        .maximum(5000)
        .schema();

pub const DEFAULT_OFFSITE_REPLICATION_HISTORY_LIMIT: u64 = 200;

fn default_offsite_history_limit() -> u64 {
    DEFAULT_OFFSITE_REPLICATION_HISTORY_LIMIT
}

fn default_zfs_stream_mode() -> OffsiteZfsStreamMode {
    OffsiteZfsStreamMode::Auto
}

fn default_install_authorized_keys() -> bool {
    true
}

pub fn verify_offsite_target_dataset(dataset: &str) -> Result<(), Error> {
    if !OFFSITE_REPLICATION_TARGET_DATASET_REGEX.is_match(dataset) {
        bail!("invalid target dataset '{}'", dataset);
    }
    for component in dataset.split('/') {
        if component == "." || component == ".." {
            bail!("target dataset must not contain '.' or '..' components");
        }
    }
    Ok(())
}

pub fn verify_offsite_ssh_user(user: &str) -> Result<(), Error> {
    if !OFFSITE_REPLICATION_SSH_USER_REGEX.is_match(user) {
        bail!("invalid SSH user '{}'", user);
    }
    Ok(())
}

pub fn verify_offsite_ssh_key_path(path: &str) -> Result<(), Error> {
    if !OFFSITE_REPLICATION_SSH_KEY_PATH_REGEX.is_match(path) {
        bail!("invalid SSH private key path '{}'", path);
    }
    for component in path.split('/').filter(|component| !component.is_empty()) {
        if component == "." || component == ".." {
            bail!("SSH private key path must not contain '.' or '..' components");
        }
    }
    Ok(())
}

pub fn verify_offsite_snapshot(snapshot: &str) -> Result<(), Error> {
    if !OFFSITE_REPLICATION_SNAPSHOT_REGEX.is_match(snapshot) {
        bail!("invalid recovery snapshot '{}'", snapshot);
    }
    let (dataset, _) = snapshot
        .rsplit_once('@')
        .ok_or_else(|| anyhow::format_err!("recovery snapshot is missing '@' separator"))?;
    verify_offsite_target_dataset(dataset)
}

pub fn verify_offsite_recovered_name(name: &str) -> Result<(), Error> {
    if !OFFSITE_REPLICATION_RECOVERED_NAME_REGEX.is_match(name) {
        bail!("invalid recovered guest name '{}'", name);
    }
    Ok(())
}

#[api]
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Replication stream mode used for ZFS send/receive.
pub enum OffsiteZfsStreamMode {
    /// Automatically choose raw mode when source datasets are encrypted.
    #[default]
    #[serde(rename = "auto")]
    Auto,
    /// Always use plain stream mode.
    #[serde(rename = "plain")]
    Plain,
    /// Always use raw stream mode (`zfs send -w`) for encrypted datasets.
    #[serde(rename = "raw")]
    Raw,
}

impl OffsiteZfsStreamMode {
    /// Returns kebab-case string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            OffsiteZfsStreamMode::Auto => "auto",
            OffsiteZfsStreamMode::Plain => "plain",
            OffsiteZfsStreamMode::Raw => "raw",
        }
    }
}

#[api(
    properties: {
        "id": { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        "target-dataset": { schema: OFFSITE_REPLICATION_TARGET_DATASET_SCHEMA },
        "source-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "target-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "ssh-private-key": { schema: OFFSITE_REPLICATION_SSH_KEY_PATH_SCHEMA },
    },
)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Off-site replication job definition managed by PDM.
pub struct OffsiteReplicationJob {
    /// Job ID.
    pub id: String,
    /// Source PVE remote.
    pub source_remote: String,
    /// Source node name.
    pub source_node: String,
    /// Guest type.
    pub guest_type: GuestType,
    /// Guest ID.
    pub vmid: u32,
    /// Target PVE remote.
    pub target_remote: String,
    /// Target node name.
    pub target_node: String,
    /// Target ZFS dataset (pool/path) on the target node.
    pub target_dataset: String,
    /// Schedule in calendar-event format.
    pub schedule: String,
    /// Maximum number of snapshots kept by pve-zsync.
    pub max_snapshots: u64,
    /// Maximum number of recorded runs retained for this job.
    #[serde(default = "default_offsite_history_limit")]
    pub history_limit: u64,
    /// Optional bandwidth limit in MiB/s.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_mib: Option<u64>,
    /// ZFS stream mode policy (`auto`, `plain`, `raw`).
    #[serde(default = "default_zfs_stream_mode")]
    pub zfs_stream_mode: OffsiteZfsStreamMode,
    /// SSH user on source node.
    pub source_user: String,
    /// SSH user on target node.
    pub target_user: String,
    /// SSH private key path on the PDM host used to execute remote commands.
    pub ssh_private_key: String,
    /// Try qemu guest-agent freeze/thaw around sync for QEMU guests.
    #[serde(default)]
    pub qga_fsfreeze: bool,
    /// Optional free-form comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Disable this job.
    #[serde(default)]
    pub disable: bool,
}

#[api(
    properties: {
        "target-dataset": { schema: OFFSITE_REPLICATION_TARGET_DATASET_SCHEMA },
        "source-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "target-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "ssh-private-key": { schema: OFFSITE_REPLICATION_SSH_KEY_PATH_SCHEMA },
    },
)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Updater payload for replacing an off-site replication job by path ID.
pub struct OffsiteReplicationJobUpdater {
    /// Source PVE remote.
    pub source_remote: String,
    /// Source node name.
    pub source_node: String,
    /// Guest type.
    pub guest_type: GuestType,
    /// Guest ID.
    pub vmid: u32,
    /// Target PVE remote.
    pub target_remote: String,
    /// Target node name.
    pub target_node: String,
    /// Target ZFS dataset (pool/path) on the target node.
    pub target_dataset: String,
    /// Schedule in calendar-event format.
    pub schedule: String,
    /// Maximum number of snapshots kept by pve-zsync.
    pub max_snapshots: u64,
    /// Maximum number of recorded runs retained for this job.
    #[serde(default = "default_offsite_history_limit")]
    pub history_limit: u64,
    /// Optional bandwidth limit in MiB/s.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_mib: Option<u64>,
    /// ZFS stream mode policy (`auto`, `plain`, `raw`).
    #[serde(default = "default_zfs_stream_mode")]
    pub zfs_stream_mode: OffsiteZfsStreamMode,
    /// SSH user on source node.
    pub source_user: String,
    /// SSH user on target node.
    pub target_user: String,
    /// SSH private key path on the PDM host used to execute remote commands.
    pub ssh_private_key: String,
    /// Try qemu guest-agent freeze/thaw around sync for QEMU guests.
    #[serde(default)]
    pub qga_fsfreeze: bool,
    /// Optional free-form comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Disable this job.
    #[serde(default)]
    pub disable: bool,
}

#[api]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Runtime status for an off-site replication job.
pub struct OffsiteReplicationRuntimeStatus {
    /// Whether the job currently has an active worker task.
    pub running: bool,
    /// Last start time (epoch seconds), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<i64>,
    /// Last successful completion time (epoch seconds), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success: Option<i64>,
    /// Last run duration in seconds, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_duration: Option<i64>,
    /// Last transferred size in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transfer_bytes: Option<u64>,
    /// Last replicated snapshot, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_snapshot: Option<String>,
    /// Last error message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Next scheduled run time (epoch seconds), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run: Option<i64>,
    /// Number of recorded runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_count: Option<u64>,
    /// Number of recorded failed runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_count: Option<u64>,
}

#[api(
    properties: {
        job: {
            type: OffsiteReplicationJob,
            flatten: true,
        },
        status: {
            type: OffsiteReplicationRuntimeStatus,
        },
    }
)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Job plus runtime status.
pub struct OffsiteReplicationJobStatus {
    /// Job configuration.
    #[serde(flatten)]
    pub job: OffsiteReplicationJob,
    /// Runtime status.
    pub status: OffsiteReplicationRuntimeStatus,
}

#[api]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// A single replication run record.
pub struct OffsiteReplicationRun {
    /// Start time (epoch seconds).
    pub start_time: i64,
    /// End time (epoch seconds).
    pub end_time: i64,
    /// Total duration in seconds.
    pub duration: i64,
    /// Whether the run completed successfully.
    pub success: bool,
    /// Parsed transfer mode (`full` or `incremental`), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_mode: Option<String>,
    /// Source snapshot used for the transfer, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_snapshot: Option<String>,
    /// Replicated snapshot/recovery point, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
    /// Estimated transfer size in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<u64>,
    /// Actual transferred size in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transferred_bytes: Option<u64>,
    /// Optional error text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Combined output from the executed command.
    pub output: String,
}

#[api]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// A recovery point derived from a successful off-site replication run.
pub struct OffsiteRecoveryPoint {
    /// Recovery snapshot identifier as recorded by the replication job.
    pub snapshot: String,
    /// Completion time of the successful replication run.
    pub end_time: i64,
    /// Parsed transfer mode (`full` or `incremental`), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_mode: Option<String>,
    /// Estimated transfer size in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<u64>,
    /// Actual transferred size in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transferred_bytes: Option<u64>,
}

#[api(
    properties: {
        "snapshot": { schema: OFFSITE_REPLICATION_SNAPSHOT_SCHEMA },
        "recovered-name": {
            schema: OFFSITE_REPLICATION_RECOVERED_NAME_SCHEMA,
            optional: true,
        },
    },
)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Parameters for promoting a replicated recovery point on the target remote.
pub struct OffsiteFailoverRequest {
    /// Recovery snapshot identifier to promote.
    pub snapshot: String,
    /// VMID to create on the recovery target.
    pub recovery_vmid: u32,
    /// Optional recovered guest name override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovered_name: Option<String>,
    /// Start the guest after registration on the target.
    #[serde(default)]
    pub start_guest: bool,
}

#[api(
    properties: {
        "source-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "target-user": { schema: OFFSITE_REPLICATION_SSH_USER_SCHEMA },
        "ssh-private-key": { schema: OFFSITE_REPLICATION_SSH_KEY_PATH_SCHEMA },
    },
)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Parameters for SSH setup preparation for off-site replication jobs.
pub struct OffsiteSshPrepareRequest {
    /// Source PVE remote.
    pub source_remote: String,
    /// Source node name.
    pub source_node: String,
    /// Source guest VMID used for permission checks.
    pub vmid: u32,
    /// Target PVE remote.
    pub target_remote: String,
    /// Target node name.
    pub target_node: String,
    /// SSH user on source node.
    pub source_user: String,
    /// SSH user on target node.
    pub target_user: String,
    /// SSH private key path on the PDM host.
    pub ssh_private_key: String,
    /// Install/verify public key in authorized_keys on both nodes.
    #[serde(default = "default_install_authorized_keys")]
    pub install_authorized_keys: bool,
    /// Copy private key material to source node for source->target pve-zsync hop.
    #[serde(default)]
    pub copy_key_to_source: bool,
}

#[api]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// One step result for SSH setup preparation.
pub struct OffsiteSshPrepareStep {
    /// Stable machine-readable status code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Optional field hint for UI highlighting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Step name.
    pub name: String,
    /// Whether this step succeeded.
    pub ok: bool,
    /// Short status message.
    pub message: String,
    /// Optional remediation guidance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
}

#[api(
    properties: {
        steps: {
            type: Array,
            items: { type: OffsiteSshPrepareStep },
            optional: true,
        },
    }
)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Overall SSH setup preparation result.
pub struct OffsiteSshPrepareResult {
    /// Whether all requested steps passed.
    pub ok: bool,
    /// Resolved source host.
    pub source_host: String,
    /// Resolved target host.
    pub target_host: String,
    /// Step-by-step execution results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<OffsiteSshPrepareStep>,
}

#[api(
    properties: {
        "ssh-private-key": { schema: OFFSITE_REPLICATION_SSH_KEY_PATH_SCHEMA },
    },
)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Parameters for generating an SSH keypair on the PDM host.
pub struct OffsiteSshKeygenRequest {
    /// Source PVE remote for privilege check.
    pub source_remote: String,
    /// Source guest VMID used for permission checks.
    pub vmid: u32,
    /// Target PVE remote for privilege check.
    pub target_remote: String,
    /// SSH private key path to create on PDM host.
    pub ssh_private_key: String,
    /// Whether an existing keypair may be replaced.
    #[serde(default)]
    pub overwrite: bool,
}

#[api]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Result for SSH key generation request.
pub struct OffsiteSshKeygenResult {
    /// Whether key generation finished successfully.
    pub ok: bool,
    /// Result status code: created, overwritten, exists, or failed.
    pub status: String,
    /// Effective SSH private key path on the PDM host.
    pub ssh_private_key: String,
    /// Optional fingerprint of resulting public key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// User-facing status message.
    pub message: String,
}

#[api(
    properties: {
        jobs: {
            type: Array,
            items: {
                type: OffsiteReplicationJob,
            },
            optional: true,
        },
    }
)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Stored off-site replication configuration.
pub struct OffsiteReplicationConfig {
    /// Job list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<OffsiteReplicationJob>,
}
