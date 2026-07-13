use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::time::Instant;

use anyhow::{bail, Context, Error};
use http::uri::Authority;
use proxmox_rest_server::{TaskState, WorkerTask};
use proxmox_time::CalendarEvent;
use serde::{Deserialize, Serialize};

use pdm_api_types::{
    verify_offsite_recovered_name, verify_offsite_snapshot, verify_offsite_ssh_key_path,
    verify_offsite_ssh_user, verify_offsite_target_dataset, Authid, OffsiteFailbackLineage,
    OffsiteFailbackPrecheck, OffsiteFailbackRequest, OffsiteFailbackRestoreMode,
    OffsiteFailoverLifecycle, OffsiteFailoverRecord, OffsiteFailoverRequest, OffsiteGuestState,
    OffsiteRecoveryPoint, OffsiteReplicationJob, OffsiteReplicationJobStatus,
    OffsiteReplicationRun, OffsiteReplicationRuntimeStatus, OffsiteSshKeygenRequest,
    OffsiteSshKeygenResult, OffsiteSshPrepareRequest, OffsiteSshPrepareResult,
    OffsiteSshPrepareStep, DEFAULT_OFFSITE_REPLICATION_HISTORY_LIMIT, PROXMOX_SAFE_ID_REGEX,
};

use crate::jobstate::{self, Job, JobState};
use crate::remote_cache::RemoteMappingCache;

const WORKER_TYPE: &str = "offsite-replication";
const FAILOVER_WORKER_TYPE: &str = "offsite-failover";
const FAILBACK_WORKER_TYPE: &str = "offsite-failback";
const HISTORY_DIR: &str = concat!(
    pdm_buildcfg::PDM_STATE_DIR_M!(),
    "/offsite-replication-history"
);
const RECOVERY_CONFIG_DIR: &str = concat!(
    pdm_buildcfg::PDM_STATE_DIR_M!(),
    "/offsite-replication-configs"
);
const FAILOVER_RECORD_DIR: &str = concat!(
    pdm_buildcfg::PDM_STATE_DIR_M!(),
    "/offsite-replication-failovers"
);
const RECOVERY_POINT_DIR: &str = concat!(
    pdm_buildcfg::PDM_STATE_DIR_M!(),
    "/offsite-replication-recovery-points"
);
const LIFECYCLE_STATE_DIR: &str = concat!(
    pdm_buildcfg::PDM_STATE_DIR_M!(),
    "/offsite-replication-lifecycle"
);
const SSH_STEP_CODE_CONNECT_AUTH_FAILED: &str = "connect_auth_failed";
const SSH_STEP_CODE_CONNECT_FAILED: &str = "connect_failed";
const SSH_STEP_CODE_MISSING_PRIVATE_KEY: &str = "missing_private_key";
const SSH_STEP_CODE_UNREADABLE_PRIVATE_KEY: &str = "unreadable_private_key";
const SSH_STEP_CODE_KEY_CHECK_FAILED: &str = "key_check_failed";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct JobHistoryFile {
    runs: Vec<OffsiteReplicationRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RecoveryPointCatalog {
    points: Vec<OffsiteRecoveryPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct ReplicationLifecycleState {
    suspended: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedQemuDisk {
    key: String,
    source_volid: String,
    source_basename: String,
    attach_options: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParsedQemuConfig {
    name: Option<String>,
    settings: Vec<(String, String)>,
    disks: Vec<ParsedQemuDisk>,
}

fn sanitize_id(id: &str) -> String {
    id.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn history_path(job_id: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(HISTORY_DIR);
    path.push(format!("{}.json", sanitize_id(job_id)));
    path
}

fn recovery_config_dir_for_job(job_id: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(RECOVERY_CONFIG_DIR);
    path.push(sanitize_id(job_id));
    path
}

fn failover_record_path(job_id: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(FAILOVER_RECORD_DIR);
    path.push(format!("{}.json", sanitize_id(job_id)));
    path
}

fn recovery_point_catalog_path(job_id: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(RECOVERY_POINT_DIR);
    path.push(format!("{}.json", sanitize_id(job_id)));
    path
}

fn lifecycle_state_path(job_id: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(LIFECYCLE_STATE_DIR);
    path.push(format!("{}.json", sanitize_id(job_id)));
    path
}

fn ensure_history_dir() -> Result<(), Error> {
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(HISTORY_DIR, Some(opts), Some(opts))?;
    Ok(())
}

fn ensure_recovery_config_dir() -> Result<(), Error> {
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(RECOVERY_CONFIG_DIR, Some(opts), Some(opts))?;
    Ok(())
}

fn ensure_failover_record_dir() -> Result<(), Error> {
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(FAILOVER_RECORD_DIR, Some(opts), Some(opts))?;
    Ok(())
}

fn ensure_recovery_point_dir() -> Result<(), Error> {
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(RECOVERY_POINT_DIR, Some(opts), Some(opts))?;
    Ok(())
}

fn ensure_lifecycle_state_dir() -> Result<(), Error> {
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(LIFECYCLE_STATE_DIR, Some(opts), Some(opts))?;
    Ok(())
}

fn load_recovery_point_catalog(job_id: &str) -> Result<RecoveryPointCatalog, Error> {
    ensure_recovery_point_dir()?;
    let content = proxmox_sys::fs::file_read_optional_string(recovery_point_catalog_path(job_id))?
        .unwrap_or_default();
    if content.trim().is_empty() {
        return Ok(RecoveryPointCatalog::default());
    }
    Ok(serde_json::from_str(&content)?)
}

fn save_recovery_point_catalog(job_id: &str, catalog: &RecoveryPointCatalog) -> Result<(), Error> {
    ensure_recovery_point_dir()?;
    let raw = serde_json::to_vec_pretty(catalog)?;
    proxmox_sys::fs::replace_file(
        recovery_point_catalog_path(job_id),
        &raw,
        proxmox_product_config::default_create_options(),
        false,
    )
}

fn load_lifecycle_state(job_id: &str) -> Result<ReplicationLifecycleState, Error> {
    ensure_lifecycle_state_dir()?;
    let content = proxmox_sys::fs::file_read_optional_string(lifecycle_state_path(job_id))?
        .unwrap_or_default();
    if content.trim().is_empty() {
        return Ok(ReplicationLifecycleState::default());
    }
    Ok(serde_json::from_str(&content)?)
}

fn save_lifecycle_state(job_id: &str, state: &ReplicationLifecycleState) -> Result<(), Error> {
    ensure_lifecycle_state_dir()?;
    let raw = serde_json::to_vec_pretty(state)?;
    proxmox_sys::fs::replace_file(
        lifecycle_state_path(job_id),
        &raw,
        proxmox_product_config::default_create_options(),
        false,
    )
}

fn suspend_replication_job(job_id: &str, record_id: &str) -> Result<(), Error> {
    save_lifecycle_state(
        job_id,
        &ReplicationLifecycleState {
            suspended: true,
            reason: Some(format!(
                "promotion '{record_id}' is active; fail back or abandon it before resuming replication"
            )),
            record_id: Some(record_id.to_string()),
        },
    )
}

fn resume_replication_job(job_id: &str) -> Result<(), Error> {
    save_lifecycle_state(job_id, &ReplicationLifecycleState::default())
}

fn recovery_config_path(job_id: &str, snapshot: &str) -> std::path::PathBuf {
    let mut path = recovery_config_dir_for_job(job_id);
    path.push(format!("{}.conf", sanitize_id(snapshot)));
    path
}

fn failover_record_id(record: &OffsiteFailoverRecord) -> String {
    format!(
        "{}-{}-{}",
        sanitize_id(&record.job_id),
        record.recovery_vmid,
        record.failover_time
    )
}

fn normalize_failover_record(record: &mut OffsiteFailoverRecord) {
    if record.record_id.is_empty() {
        record.record_id = failover_record_id(record);
    }
    if record.failback_time.is_some() && record.lifecycle == OffsiteFailoverLifecycle::Active {
        record.lifecycle = OffsiteFailoverLifecycle::Returned;
    }
}

fn supersede_reused_recovery_vmids(records: &mut [OffsiteFailoverRecord]) {
    let mut newest_active_by_vmid: HashMap<u32, i64> = HashMap::new();
    for record in records.iter() {
        if record.lifecycle == OffsiteFailoverLifecycle::Active {
            newest_active_by_vmid
                .entry(record.recovery_vmid)
                .and_modify(|time| *time = (*time).max(record.failover_time))
                .or_insert(record.failover_time);
        }
    }
    for record in records {
        if record.lifecycle == OffsiteFailoverLifecycle::Active
            && newest_active_by_vmid
                .get(&record.recovery_vmid)
                .is_some_and(|time| record.failover_time < *time)
        {
            record.lifecycle = OffsiteFailoverLifecycle::Superseded;
            record.status_message = Some(format!(
                "superseded by a later promotion using recovery VMID {}",
                record.recovery_vmid
            ));
        }
    }
}

pub fn list_failover_records(job_id: &str) -> Result<Vec<OffsiteFailoverRecord>, Error> {
    ensure_failover_record_dir()?;
    let path = failover_record_path(job_id);
    let content = proxmox_sys::fs::file_read_optional_string(path)?.unwrap_or_default();
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut records: Vec<OffsiteFailoverRecord> = serde_json::from_str(&content)?;
    for record in &mut records {
        normalize_failover_record(record);
    }
    supersede_reused_recovery_vmids(&mut records);
    Ok(records)
}

fn save_failover_record(mut record: OffsiteFailoverRecord) -> Result<(), Error> {
    ensure_failover_record_dir()?;
    let path = failover_record_path(&record.job_id);
    let mut records = list_failover_records(&record.job_id)?;
    normalize_failover_record(&mut record);
    for entry in &mut records {
        if entry.lifecycle == OffsiteFailoverLifecycle::Active
            && entry.recovery_vmid == record.recovery_vmid
            && entry.record_id != record.record_id
        {
            entry.lifecycle = OffsiteFailoverLifecycle::Superseded;
            entry.status_message = Some(format!(
                "superseded when recovery VMID {} was promoted again",
                record.recovery_vmid
            ));
        }
    }
    records.retain(|entry| entry.record_id != record.record_id);
    records.push(record);

    let raw = serde_json::to_string_pretty(&records)?;
    proxmox_product_config::replace_config(path, raw.as_bytes())
}

fn mark_failback_complete(
    job_id: &str,
    recovery_vmid: u32,
    source_restore_vmid: u32,
    target_cleaned: bool,
) -> Result<(), Error> {
    ensure_failover_record_dir()?;
    let path = failover_record_path(job_id);
    let mut records = list_failover_records(job_id)?;
    let record = records
        .iter_mut()
        .filter(|record| record.recovery_vmid == recovery_vmid)
        .max_by_key(|record| record.failover_time)
        .with_context(|| format!("no failover record found for recovery VMID {recovery_vmid}"))?;
    record.failback_time = Some(proxmox_time::epoch_i64());
    record.source_restore_vmid = Some(source_restore_vmid);
    record.target_cleaned = target_cleaned;
    record.lifecycle = OffsiteFailoverLifecycle::Returned;
    record.status_message = Some(format!("returned to source VMID {source_restore_vmid}"));

    let raw = serde_json::to_string_pretty(&records)?;
    proxmox_product_config::replace_config(path, raw.as_bytes())?;
    if !records
        .iter()
        .any(|record| record.lifecycle == OffsiteFailoverLifecycle::Active)
    {
        if target_cleaned {
            resume_replication_job(job_id)?;
        } else {
            save_lifecycle_state(
                job_id,
                &ReplicationLifecycleState {
                    suspended: true,
                    reason: Some(format!(
                        "failback completed, but promoted VMID {recovery_vmid} remains on the target; remove it before resuming replication"
                    )),
                    record_id: None,
                },
            )?;
        }
    }
    Ok(())
}

pub fn abandon_failover_record(job: &OffsiteReplicationJob, record_id: &str) -> Result<(), Error> {
    ensure_failover_record_dir()?;
    let path = failover_record_path(&job.id);
    let mut records = list_failover_records(&job.id)?;
    let recovery_vmid = {
        let record = records
            .iter_mut()
            .find(|record| record.record_id == record_id)
            .with_context(|| format!("failover record '{record_id}' does not exist"))?;
        if record.lifecycle != OffsiteFailoverLifecycle::Active {
            bail!(
                "failover record '{}' is not active (state: {:?})",
                record_id,
                record.lifecycle
            );
        }
        record.lifecycle = OffsiteFailoverLifecycle::Abandoned;
        record.abandoned_time = Some(proxmox_time::epoch_i64());
        record.status_message = Some("promotion was archived by an operator".to_string());
        record.recovery_vmid
    };
    let worker_name = format!("{}-{recovery_vmid}", job.id);
    let _failback_guard = Job::new(FAILBACK_WORKER_TYPE, &worker_name).with_context(|| {
        format!("cannot archive recovery VMID {recovery_vmid} while failback is running")
    })?;

    let raw = serde_json::to_string_pretty(&records)?;
    proxmox_product_config::replace_config(path, raw.as_bytes())?;
    if !records
        .iter()
        .any(|record| record.lifecycle == OffsiteFailoverLifecycle::Active)
    {
        let source_state = query_guest_state(
            &job.source_remote,
            &job.source_node,
            &job.source_user,
            &job.ssh_private_key,
            job.vmid,
        )?;
        let target_state = query_guest_state(
            &job.target_remote,
            &job.target_node,
            &job.target_user,
            &job.ssh_private_key,
            recovery_vmid,
        )?;
        if matches!(
            source_state,
            OffsiteGuestState::Running | OffsiteGuestState::Stopped
        ) && target_state == OffsiteGuestState::Missing
        {
            resume_replication_job(&job.id)?;
        } else {
            let reason = if target_state != OffsiteGuestState::Missing {
                format!(
                    "promotion '{record_id}' was archived, but target VMID {} still exists",
                    recovery_vmid
                )
            } else {
                format!(
                    "promotion '{record_id}' was archived, but source VMID {} is unavailable",
                    job.vmid
                )
            };
            save_lifecycle_state(
                &job.id,
                &ReplicationLifecycleState {
                    suspended: true,
                    reason: Some(reason),
                    record_id: None,
                },
            )?;
        }
    }
    Ok(())
}

pub fn resume_suspended_replication(job: &OffsiteReplicationJob) -> Result<(), Error> {
    let records = list_failover_records(&job.id)?;
    if records
        .iter()
        .any(|record| record.lifecycle == OffsiteFailoverLifecycle::Active)
    {
        bail!("an active promoted guest must be failed back or abandoned first");
    }
    for record in records.iter().filter(|record| {
        matches!(
            record.lifecycle,
            OffsiteFailoverLifecycle::Returned | OffsiteFailoverLifecycle::Abandoned
        ) && !record.target_cleaned
    }) {
        let target_state = query_guest_state(
            &job.target_remote,
            &job.target_node,
            &job.target_user,
            &job.ssh_private_key,
            record.recovery_vmid,
        )?;
        if matches!(
            target_state,
            OffsiteGuestState::Running | OffsiteGuestState::Stopped
        ) {
            bail!(
                "returned target VMID {} still exists; remove it before resuming replication",
                record.recovery_vmid
            );
        }
    }
    let source_state = query_guest_state(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        job.vmid,
    )?;
    if !matches!(
        source_state,
        OffsiteGuestState::Running | OffsiteGuestState::Stopped
    ) {
        bail!(
            "source VMID {} is unavailable; restore the source guest before resuming replication",
            job.vmid
        );
    }
    resume_replication_job(&job.id)
}

pub fn ensure_job_can_be_removed(job: &OffsiteReplicationJob) -> Result<(), Error> {
    for record in list_failover_records(&job.id)? {
        if record.lifecycle == OffsiteFailoverLifecycle::Active {
            bail!(
                "active promoted VMID {} must be failed back or abandoned before deleting job '{}'",
                record.recovery_vmid,
                job.id
            );
        }
        if matches!(
            record.lifecycle,
            OffsiteFailoverLifecycle::Returned | OffsiteFailoverLifecycle::Abandoned
        ) && !record.target_cleaned
        {
            let target_state = query_guest_state(
                &job.target_remote,
                &job.target_node,
                &job.target_user,
                &job.ssh_private_key,
                record.recovery_vmid,
            )?;
            if target_state != OffsiteGuestState::Missing {
                bail!(
                    "promoted VMID {} remains on the target; remove it before deleting job '{}'",
                    record.recovery_vmid,
                    job.id
                );
            }
        }
    }
    Ok(())
}

pub fn ensure_lifecycle_safe_update(
    current: &OffsiteReplicationJob,
    updated: &OffsiteReplicationJob,
) -> Result<(), Error> {
    if !load_lifecycle_state(&current.id)?.suspended {
        return Ok(());
    }

    let identity_changed = current.source_remote != updated.source_remote
        || current.source_node != updated.source_node
        || current.guest_type != updated.guest_type
        || current.vmid != updated.vmid
        || current.target_remote != updated.target_remote
        || current.target_node != updated.target_node
        || current.target_dataset != updated.target_dataset
        || current.source_user != updated.source_user
        || current.target_user != updated.target_user
        || current.ssh_private_key != updated.ssh_private_key;
    if identity_changed {
        bail!(
            "source, target, guest, and SSH identity fields cannot change while recovery lifecycle for job '{}' is suspended",
            current.id
        );
    }
    Ok(())
}

fn load_history(job_id: &str) -> Result<JobHistoryFile, Error> {
    ensure_history_dir()?;
    let path = history_path(job_id);
    let content = proxmox_sys::fs::file_read_optional_string(path)?.unwrap_or_default();
    if content.trim().is_empty() {
        return Ok(JobHistoryFile::default());
    }
    Ok(serde_json::from_str(&content)?)
}

fn save_history(job_id: &str, history: &JobHistoryFile) -> Result<(), Error> {
    ensure_history_dir()?;
    let path = history_path(job_id);
    let raw = serde_json::to_vec_pretty(history)?;
    proxmox_sys::fs::replace_file(
        path,
        &raw,
        proxmox_product_config::default_create_options(),
        false,
    )
}

fn normalize_history_limit(limit: u64) -> usize {
    let default_limit = DEFAULT_OFFSITE_REPLICATION_HISTORY_LIMIT as usize;
    let clamped = usize::try_from(limit).unwrap_or(default_limit);
    clamped.max(1)
}

fn append_history(
    job_id: &str,
    history_limit: usize,
    run: OffsiteReplicationRun,
) -> Result<(), Error> {
    let mut history = load_history(job_id)?;
    history.runs.push(annotate_run(run));
    if history.runs.len() > history_limit {
        let to_trim = history.runs.len() - history_limit;
        history.runs.drain(0..to_trim);
    }
    save_history(job_id, &history)
}

fn parse_byte_size(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    let text = text.strip_suffix('B').unwrap_or(text).trim();
    let (number, factor) = match text.chars().last() {
        Some(last) if last.is_ascii_alphabetic() => {
            let factor = match last.to_ascii_uppercase() {
                'K' => 1024_u64,
                'M' => 1024_u64.pow(2),
                'G' => 1024_u64.pow(3),
                'T' => 1024_u64.pow(4),
                'P' => 1024_u64.pow(5),
                _ => return None,
            };
            (&text[..text.len().saturating_sub(1)], factor)
        }
        _ => (text, 1_u64),
    };

    let value = number.trim().parse::<f64>().ok()?;
    Some((value * factor as f64).round() as u64)
}

fn parse_transfer_table_line(line: &str, run: &mut OffsiteReplicationRun) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return;
    }

    if !parts[0].contains(':') || !parts[2].contains('@') {
        return;
    }

    if run.transferred_bytes.is_none() {
        run.transferred_bytes = parse_byte_size(parts[1]);
    }

    if run.snapshot.is_none() {
        run.snapshot = Some(parts[2..].join(" "));
    }
}

fn annotate_run(mut run: OffsiteReplicationRun) -> OffsiteReplicationRun {
    let output = run.output.clone();

    for raw_line in output.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("full send of ") {
            if let Some((snapshot, size)) = rest.split_once(" estimated size is ") {
                run.transfer_mode = Some("full".to_string());
                run.source_snapshot = Some(snapshot.trim().to_string());
                if run.snapshot.is_none() {
                    run.snapshot = Some(snapshot.trim().to_string());
                }
                run.estimated_bytes = parse_byte_size(size);
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("send from ") {
            if let Some((source_snapshot, remaining)) = rest.split_once(" to ") {
                if let Some((snapshot, size)) = remaining.split_once(" estimated size is ") {
                    run.transfer_mode = Some("incremental".to_string());
                    run.source_snapshot = Some(source_snapshot.trim().to_string());
                    run.snapshot = Some(snapshot.trim().to_string());
                    run.estimated_bytes = parse_byte_size(size);
                }
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("total estimated size is ") {
            if run.estimated_bytes.is_none() {
                run.estimated_bytes = parse_byte_size(rest);
            }
            continue;
        }

        if line.starts_with("TIME") && line.contains("SENT") && line.contains("SNAPSHOT") {
            continue;
        }

        parse_transfer_table_line(line, &mut run);
    }

    // Some `pve-zsync` versions do not emit a parseable transfer row. Keep the history table
    // useful by falling back to the parsed estimate for successful runs.
    if run.success && run.transferred_bytes.is_none() {
        run.transferred_bytes = run.estimated_bytes;
    }

    run
}

fn parse_authority(host: &str) -> (String, Option<u16>) {
    let authority = host
        .parse::<Authority>()
        .ok()
        .or_else(|| format!("{host}:22").parse::<Authority>().ok());

    match authority {
        Some(authority) => (
            authority.host().to_string(),
            authority.port_u16().or(Some(22)),
        ),
        None => (host.to_string(), Some(22)),
    }
}

fn resolve_node_host(remote: &str, node: &str) -> Result<(String, Option<u16>), Error> {
    let cache = RemoteMappingCache::get();
    if let Some(info) = cache.info_by_node_name(remote, node) {
        return Ok(parse_authority(&info.hostname));
    }

    let (config, _) = pdm_config::remotes::config()?;
    let Some(remote_cfg) = config.get(remote) else {
        bail!("remote '{remote}' not found");
    };

    if let Some(entry) = remote_cfg.nodes.first() {
        return Ok(parse_authority(&entry.hostname));
    }

    bail!("remote '{remote}' has no configured node endpoint");
}

fn shell_escape(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\"'\"'"))
}

fn normalize_host_for_connection(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn normalize_ssh_port(port: Option<u16>) -> Option<u16> {
    match port {
        // Remote endpoint configuration stores API ports, but the off-site replication worker
        // SSHes into the PVE nodes directly.
        Some(8006 | 8007) | None => None,
        Some(port) => Some(port),
    }
}

fn ssh_target_for_script(user: &str, host: &str) -> String {
    shell_escape(&format!("{}@{}", user, normalize_host_for_connection(host)))
}

fn ssh_port_args_for_script(port: Option<u16>) -> String {
    normalize_ssh_port(port)
        .map(|port| format!(" -p {port}"))
        .unwrap_or_default()
}

fn merge_command_output(output: &std::process::Output) -> String {
    let mut merged = String::new();
    if !output.stdout.is_empty() {
        merged.push_str(&String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        if !merged.is_empty() {
            merged.push('\n');
        }
        merged.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    merged
}

fn query_guest_state(
    remote: &str,
    node: &str,
    user: &str,
    ssh_private_key: &str,
    vmid: u32,
) -> Result<OffsiteGuestState, Error> {
    let script = format!(
        "if qm status {vmid}; then exit 0; fi\n\
         if [ ! -e /etc/pve/qemu-server/{vmid}.conf ]; then\n\
           echo PDM_GUEST_MISSING\n\
           exit 0\n\
         fi\n\
         exit 1\n"
    );
    let output = run_ssh_script(remote, node, user, ssh_private_key, &script)?;
    if !output.status.success() {
        let detail = merge_command_output(&output);
        bail!(
            "failed to query QEMU VMID {vmid} on '{remote}/{node}': {}",
            if detail.trim().is_empty() {
                "remote command failed".to_string()
            } else {
                detail.trim().to_string()
            }
        );
    }
    let status = String::from_utf8_lossy(&output.stdout);
    if status.lines().any(|line| line == "PDM_GUEST_MISSING") {
        Ok(OffsiteGuestState::Missing)
    } else if status.contains("status: running") {
        Ok(OffsiteGuestState::Running)
    } else if status.contains("status: stopped") {
        Ok(OffsiteGuestState::Stopped)
    } else {
        Ok(OffsiteGuestState::Unknown)
    }
}

fn run_ssh_script(
    remote: &str,
    node: &str,
    user: &str,
    ssh_private_key: &str,
    script: &str,
) -> Result<std::process::Output, Error> {
    let (host, port) = resolve_node_host(remote, node)?;
    let host = normalize_host_for_connection(&host);

    let mut command = Command::new("ssh");
    command
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg("-i")
        .arg(ssh_private_key);

    if let Some(port) = normalize_ssh_port(port) {
        command.arg("-p").arg(port.to_string());
    }

    command
        .arg(format!("{user}@{host}"))
        .arg("--")
        .arg("/bin/bash")
        .arg("-lc")
        .arg(shell_escape(script));

    command
        .output()
        .with_context(|| format!("failed to execute ssh against '{remote}/{node}'"))
}

fn fetch_guest_config(job: &OffsiteReplicationJob) -> Result<String, Error> {
    let command = match job.guest_type {
        pdm_api_types::resource::GuestType::Qemu => format!("qm config {} --current", job.vmid),
        pdm_api_types::resource::GuestType::Lxc => format!("pct config {}", job.vmid),
    };

    let output = run_ssh_script(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        &command,
    )?;
    let merged = merge_command_output(&output);

    if !output.status.success() {
        bail!(
            "failed to capture guest config for '{}': {}",
            job.id,
            if merged.trim().is_empty() {
                "ssh command failed".to_string()
            } else {
                merged
            }
        );
    }

    Ok(merged)
}

fn fetch_qemu_config(
    remote: &str,
    node: &str,
    user: &str,
    ssh_private_key: &str,
    vmid: u32,
) -> Result<String, Error> {
    let output = run_ssh_script(
        remote,
        node,
        user,
        ssh_private_key,
        &format!("qm config {vmid} --current"),
    )?;
    let merged = merge_command_output(&output);

    if !output.status.success() {
        bail!(
            "failed to capture QEMU config for VMID {vmid}: {}",
            if merged.trim().is_empty() {
                "ssh command failed".to_string()
            } else {
                merged
            }
        );
    }

    Ok(merged)
}

fn save_recovery_guest_config(job_id: &str, snapshot: &str, config: &str) -> Result<(), Error> {
    ensure_recovery_config_dir()?;
    let path = recovery_config_path(job_id, snapshot);
    let parent = path
        .parent()
        .context("failed to derive recovery config parent directory")?;
    let mode = nix::sys::stat::Mode::from_bits_truncate(0o0750);
    let opts = proxmox_product_config::default_create_options().perm(mode);
    proxmox_sys::fs::create_path(parent, Some(opts), Some(opts))?;
    proxmox_sys::fs::replace_file(
        path,
        config.as_bytes(),
        proxmox_product_config::default_create_options(),
        false,
    )
}

pub fn load_recovery_guest_config(job_id: &str, snapshot: &str) -> Result<String, Error> {
    let path = recovery_config_path(job_id, snapshot);
    proxmox_sys::fs::file_read_string(path)
}

fn run_ssh_script_checked(
    remote: &str,
    node: &str,
    user: &str,
    ssh_private_key: &str,
    script: &str,
) -> Result<String, Error> {
    let output = run_ssh_script(remote, node, user, ssh_private_key, script)?;
    let merged = merge_command_output(&output);

    if !output.status.success() {
        bail!(
            "{}",
            if merged.trim().is_empty() {
                format!("ssh command against '{remote}/{node}' failed")
            } else {
                merged
            }
        );
    }

    Ok(merged)
}

fn ssh_prepare_step(name: &str, ok: bool, message: String) -> OffsiteSshPrepareStep {
    OffsiteSshPrepareStep {
        code: Some(name.to_string()),
        field: None,
        name: name.to_string(),
        ok,
        message,
        remediation: None,
    }
}

fn ssh_prepare_step_err(
    name: &str,
    message: String,
    remediation: Option<String>,
) -> OffsiteSshPrepareStep {
    OffsiteSshPrepareStep {
        code: Some(name.to_string()),
        field: None,
        name: name.to_string(),
        ok: false,
        message,
        remediation,
    }
}

fn ssh_prepare_step_skipped(
    name: &str,
    message: String,
    remediation: Option<String>,
) -> OffsiteSshPrepareStep {
    OffsiteSshPrepareStep {
        code: Some("skipped".to_string()),
        field: None,
        name: name.to_string(),
        ok: false,
        message,
        remediation,
    }
}

fn ssh_prepare_step_err_with_code(
    name: &str,
    code: &str,
    field: Option<&str>,
    message: String,
    remediation: Option<String>,
) -> OffsiteSshPrepareStep {
    OffsiteSshPrepareStep {
        code: Some(code.to_string()),
        field: field.map(str::to_string),
        name: name.to_string(),
        ok: false,
        message,
        remediation,
    }
}

fn ssh_prepare_user_remediation(remote: &str, node: &str, user: &str) -> String {
    if user == "root" {
        return format!(
            "On {remote}/{node}, ensure root SSH login is allowed and the public key is present.\n\
             Example:\n\
             install -d -m 700 /root/.ssh\n\
             touch /root/.ssh/authorized_keys\n\
             chmod 600 /root/.ssh/authorized_keys\n\
             chown -R root:root /root/.ssh\n\
             # verify sshd allows key auth for root (PermitRootLogin + PubkeyAuthentication)\n\
             # then restart sshd"
        );
    }

    format!(
        "On {remote}/{node}, create or unlock user '{user}' and install the public key.\n\
         Example:\n\
         useradd -m -s /bin/bash {user} || true\n\
         install -d -m 700 ~{user}/.ssh\n\
         touch ~{user}/.ssh/authorized_keys\n\
         chown -R {user}:{user} ~{user}/.ssh\n\
         chmod 600 ~{user}/.ssh/authorized_keys"
    )
}

fn read_public_key_for_prepare(ssh_private_key: &str) -> Result<String, Error> {
    let private_path = std::path::Path::new(ssh_private_key);
    if !private_path.exists() {
        bail!("private key '{}' does not exist", ssh_private_key);
    }

    let public_path = format!("{ssh_private_key}.pub");
    if let Ok(public) = std::fs::read_to_string(&public_path) {
        let line = public.lines().next().unwrap_or("").trim();
        if !line.is_empty() {
            return Ok(line.to_string());
        }
    }

    let output = Command::new("ssh-keygen")
        .arg("-y")
        .arg("-f")
        .arg(ssh_private_key)
        .output()
        .with_context(|| format!("failed to derive public key for '{}'", ssh_private_key))?;

    if !output.status.success() {
        bail!(
            "failed to derive public key for '{}': {}",
            ssh_private_key,
            merge_command_output(&output)
        );
    }

    let public = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if public.is_empty() {
        bail!("derived public key for '{}' is empty", ssh_private_key);
    }

    Ok(public)
}

fn key_check_error_code(message: &str) -> &'static str {
    if message.contains("does not exist") {
        SSH_STEP_CODE_MISSING_PRIVATE_KEY
    } else if message.contains("Permission denied") || message.contains("not readable") {
        SSH_STEP_CODE_UNREADABLE_PRIVATE_KEY
    } else {
        SSH_STEP_CODE_KEY_CHECK_FAILED
    }
}

fn connect_error_code(message: &str) -> &'static str {
    if message.contains("Permission denied") {
        SSH_STEP_CODE_CONNECT_AUTH_FAILED
    } else {
        SSH_STEP_CODE_CONNECT_FAILED
    }
}

fn create_key_parent_dir(path: &std::path::Path) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::format_err!("invalid key path '{}'", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create directory '{}'", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set permissions on '{}'", parent.display()))?;
    }
    Ok(())
}

fn ssh_keygen_fingerprint(public_key_path: &std::path::Path) -> Result<String, Error> {
    let output = Command::new("ssh-keygen")
        .arg("-lf")
        .arg(public_key_path)
        .output()
        .with_context(|| {
            format!(
                "failed to read fingerprint from '{}'",
                public_key_path.display()
            )
        })?;
    if !output.status.success() {
        bail!(
            "failed to read fingerprint: {}",
            merge_command_output(&output)
        );
    }
    let line = String::from_utf8_lossy(&output.stdout);
    let fp = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::format_err!("unable to parse key fingerprint"))?;
    Ok(fp.to_string())
}

pub fn generate_ssh_keypair(
    request: &OffsiteSshKeygenRequest,
) -> Result<OffsiteSshKeygenResult, Error> {
    verify_offsite_ssh_key_path(&request.ssh_private_key)?;

    let key_path = std::path::Path::new(&request.ssh_private_key);
    if !key_path.is_absolute() {
        bail!("ssh private key path must be absolute");
    }

    create_key_parent_dir(key_path)?;

    let pub_path = std::path::PathBuf::from(format!("{}.pub", request.ssh_private_key));
    let private_exists = key_path.exists();
    let public_exists = pub_path.exists();
    let any_exists = private_exists || public_exists;

    if any_exists && !request.overwrite {
        return Ok(OffsiteSshKeygenResult {
            ok: false,
            status: "exists".to_string(),
            ssh_private_key: request.ssh_private_key.clone(),
            fingerprint: None,
            message: format!(
                "SSH key path '{}' already exists. Confirm overwrite to replace it.",
                request.ssh_private_key
            ),
        });
    }

    if any_exists {
        if key_path.exists() {
            std::fs::remove_file(key_path)
                .with_context(|| format!("failed to remove '{}'", key_path.display()))?;
        }
        if pub_path.exists() {
            std::fs::remove_file(&pub_path)
                .with_context(|| format!("failed to remove '{}'", pub_path.display()))?;
        }
    }

    let output = Command::new("ssh-keygen")
        .arg("-q")
        .arg("-t")
        .arg("ed25519")
        .arg("-N")
        .arg("")
        .arg("-f")
        .arg(&request.ssh_private_key)
        .output()
        .with_context(|| "failed to execute ssh-keygen".to_string())?;
    if !output.status.success() {
        bail!("ssh-keygen failed: {}", merge_command_output(&output));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(key_path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set mode on '{}'", key_path.display()))?;
        std::fs::set_permissions(&pub_path, std::fs::Permissions::from_mode(0o644))
            .with_context(|| format!("failed to set mode on '{}'", pub_path.display()))?;
    }

    let fingerprint = ssh_keygen_fingerprint(&pub_path).ok();
    let overwritten = any_exists && request.overwrite;
    Ok(OffsiteSshKeygenResult {
        ok: true,
        status: if overwritten {
            "overwritten".to_string()
        } else {
            "created".to_string()
        },
        ssh_private_key: request.ssh_private_key.clone(),
        fingerprint,
        message: if overwritten {
            format!("Replaced SSH keypair at '{}'.", request.ssh_private_key)
        } else {
            format!("Created SSH keypair at '{}'.", request.ssh_private_key)
        },
    })
}

fn install_authorized_key(
    remote: &str,
    node: &str,
    user: &str,
    ssh_private_key: &str,
    public_key: &str,
) -> Result<(), Error> {
    let script = format!(
        "set -eu\n\
         install -d -m 700 \"$HOME/.ssh\"\n\
         touch \"$HOME/.ssh/authorized_keys\"\n\
         chmod 600 \"$HOME/.ssh/authorized_keys\"\n\
         if ! grep -qxF {public_key} \"$HOME/.ssh/authorized_keys\"; then\n\
           printf '%s\\n' {public_key} >> \"$HOME/.ssh/authorized_keys\"\n\
         fi\n",
        public_key = shell_escape(public_key),
    );

    run_ssh_script_checked(remote, node, user, ssh_private_key, &script)?;
    Ok(())
}

fn copy_key_to_source_host(
    request: &OffsiteSshPrepareRequest,
    source_host: &str,
    source_port: Option<u16>,
) -> Result<(), Error> {
    let key_path = std::path::Path::new(&request.ssh_private_key);
    if !key_path.exists() {
        bail!("private key '{}' does not exist", request.ssh_private_key);
    }

    let destination = format!(
        "{}@{}:{}",
        request.source_user,
        normalize_host_for_connection(source_host),
        request.ssh_private_key
    );

    let mut command = Command::new("scp");
    command
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg("-i")
        .arg(&request.ssh_private_key);
    if let Some(port) = normalize_ssh_port(source_port) {
        command.arg("-P").arg(port.to_string());
    }
    command.arg(&request.ssh_private_key).arg(&destination);

    let output = command
        .output()
        .with_context(|| "failed to copy SSH key to source host".to_string())?;
    if !output.status.success() {
        bail!(
            "failed to copy SSH key to source host: {}",
            merge_command_output(&output)
        );
    }

    let script = format!(
        "set -eu\n\
         install -d -m 700 \"$HOME/.ssh\"\n\
         chmod 600 {key}\n\
         if [ ! -s {key_pub} ]; then\n\
           ssh-keygen -y -f {key} > {key_pub}\n\
         fi\n\
         chmod 644 {key_pub}\n",
        key = shell_escape(&request.ssh_private_key),
        key_pub = shell_escape(&format!("{}.pub", request.ssh_private_key)),
    );

    run_ssh_script_checked(
        &request.source_remote,
        &request.source_node,
        &request.source_user,
        &request.ssh_private_key,
        &script,
    )?;

    Ok(())
}

fn verify_source_to_target_hop(
    request: &OffsiteSshPrepareRequest,
    target_host: &str,
    target_port: Option<u16>,
) -> Result<(), Error> {
    let target = format!(
        "{}@{}",
        request.target_user,
        normalize_host_for_connection(target_host)
    );
    let port_opt = normalize_ssh_port(target_port)
        .map(|port| format!("-p {port} "))
        .unwrap_or_default();
    let script = format!(
        "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -i {key} {port}{target} -- true",
        key = shell_escape(&request.ssh_private_key),
        port = port_opt,
        target = shell_escape(&target),
    );

    run_ssh_script_checked(
        &request.source_remote,
        &request.source_node,
        &request.source_user,
        &request.ssh_private_key,
        &script,
    )?;
    Ok(())
}

pub fn prepare_ssh(request: &OffsiteSshPrepareRequest) -> Result<OffsiteSshPrepareResult, Error> {
    verify_offsite_ssh_user(&request.source_user)?;
    verify_offsite_ssh_user(&request.target_user)?;
    verify_offsite_ssh_key_path(&request.ssh_private_key)?;

    let (source_host, source_port) =
        resolve_node_host(&request.source_remote, &request.source_node)
            .with_context(|| "failed to resolve source node host")?;
    let (target_host, target_port) =
        resolve_node_host(&request.target_remote, &request.target_node)
            .with_context(|| "failed to resolve target node host")?;

    let mut result = OffsiteSshPrepareResult {
        ok: false,
        source_host: source_host.clone(),
        target_host: target_host.clone(),
        steps: Vec::new(),
    };

    let public_key = match read_public_key_for_prepare(&request.ssh_private_key) {
        Ok(key) => {
            result.steps.push(ssh_prepare_step(
                "key-check",
                true,
                "SSH private/public key on PDM host is usable.".to_string(),
            ));
            key
        }
        Err(err) => {
            let message = err.to_string();
            result.steps.push(ssh_prepare_step_err_with_code(
                "key-check",
                key_check_error_code(&message),
                Some("ssh-private-key"),
                message,
                Some(format!(
                    "Ensure '{}' exists and is readable by the PDM service user.",
                    request.ssh_private_key
                )),
            ));
            return Ok(result);
        }
    };

    let source_connected = match run_ssh_script_checked(
        &request.source_remote,
        &request.source_node,
        &request.source_user,
        &request.ssh_private_key,
        "id -un >/dev/null",
    ) {
        Ok(_) => {
            result.steps.push(ssh_prepare_step(
                "source-connect",
                true,
                "Connected to source node over SSH.".to_string(),
            ));
            true
        }
        Err(err) => {
            let message = err.to_string();
            result.steps.push(ssh_prepare_step_err_with_code(
                "source-connect",
                connect_error_code(&message),
                Some("source-user"),
                message,
                Some(ssh_prepare_user_remediation(
                    &request.source_remote,
                    &request.source_node,
                    &request.source_user,
                )),
            ));
            false
        }
    };

    let target_connected = match run_ssh_script_checked(
        &request.target_remote,
        &request.target_node,
        &request.target_user,
        &request.ssh_private_key,
        "id -un >/dev/null",
    ) {
        Ok(_) => {
            result.steps.push(ssh_prepare_step(
                "target-connect",
                true,
                "Connected to target node over SSH.".to_string(),
            ));
            true
        }
        Err(err) => {
            let message = err.to_string();
            result.steps.push(ssh_prepare_step_err_with_code(
                "target-connect",
                connect_error_code(&message),
                Some("target-user"),
                message,
                Some(ssh_prepare_user_remediation(
                    &request.target_remote,
                    &request.target_node,
                    &request.target_user,
                )),
            ));
            false
        }
    };

    if request.install_authorized_keys {
        if source_connected {
            if let Err(err) = install_authorized_key(
                &request.source_remote,
                &request.source_node,
                &request.source_user,
                &request.ssh_private_key,
                &public_key,
            ) {
                result.steps.push(ssh_prepare_step_err(
                    "source-authorized-keys",
                    err.to_string(),
                    Some(ssh_prepare_user_remediation(
                        &request.source_remote,
                        &request.source_node,
                        &request.source_user,
                    )),
                ));
            } else {
                result.steps.push(ssh_prepare_step(
                    "source-authorized-keys",
                    true,
                    "Installed/verified public key on source user authorized_keys.".to_string(),
                ));
            }
        } else {
            result.steps.push(ssh_prepare_step_skipped(
                "source-authorized-keys",
                "Skipped because source-connect failed.".to_string(),
                Some(ssh_prepare_user_remediation(
                    &request.source_remote,
                    &request.source_node,
                    &request.source_user,
                )),
            ));
        }

        if target_connected {
            if let Err(err) = install_authorized_key(
                &request.target_remote,
                &request.target_node,
                &request.target_user,
                &request.ssh_private_key,
                &public_key,
            ) {
                result.steps.push(ssh_prepare_step_err(
                    "target-authorized-keys",
                    err.to_string(),
                    Some(ssh_prepare_user_remediation(
                        &request.target_remote,
                        &request.target_node,
                        &request.target_user,
                    )),
                ));
            } else {
                result.steps.push(ssh_prepare_step(
                    "target-authorized-keys",
                    true,
                    "Installed/verified public key on target user authorized_keys.".to_string(),
                ));
            }
        } else {
            result.steps.push(ssh_prepare_step_skipped(
                "target-authorized-keys",
                "Skipped because target-connect failed.".to_string(),
                Some(ssh_prepare_user_remediation(
                    &request.target_remote,
                    &request.target_node,
                    &request.target_user,
                )),
            ));
        }
    } else {
        result.steps.push(ssh_prepare_step(
            "authorized-keys",
            true,
            "authorized_keys installation skipped by option.".to_string(),
        ));
    }

    if request.copy_key_to_source {
        if source_connected {
            if let Err(err) = copy_key_to_source_host(request, &source_host, source_port) {
                result.steps.push(ssh_prepare_step_err(
                    "copy-key-to-source",
                    err.to_string(),
                    Some(format!(
                        "Ensure source user '{}' can write '{}' and retry with copy enabled.",
                        request.source_user, request.ssh_private_key
                    )),
                ));
            } else {
                result.steps.push(ssh_prepare_step(
                    "copy-key-to-source",
                    true,
                    "Copied key material to source host for source->target hop.".to_string(),
                ));
            }
        } else {
            result.steps.push(ssh_prepare_step_skipped(
                "copy-key-to-source",
                "Skipped because source-connect failed.".to_string(),
                Some(ssh_prepare_user_remediation(
                    &request.source_remote,
                    &request.source_node,
                    &request.source_user,
                )),
            ));
        }
    } else {
        result.steps.push(ssh_prepare_step(
            "copy-key-to-source",
            true,
            "Source key copy skipped by option.".to_string(),
        ));
    }

    if source_connected && target_connected {
        if let Err(err) = verify_source_to_target_hop(request, &target_host, target_port) {
            result.steps.push(ssh_prepare_step_err(
                "source-to-target-hop",
                err.to_string(),
                Some(
                    "Verify source user key access to target, or rerun with 'Copy key to source' enabled."
                        .to_string(),
                ),
            ));
        } else {
            result.steps.push(ssh_prepare_step(
                "source-to-target-hop",
                true,
                "Source host can open SSH hop to target host.".to_string(),
            ));
        }
    } else {
        result.steps.push(ssh_prepare_step_skipped(
            "source-to-target-hop",
            "Skipped because source or target connectivity failed.".to_string(),
            Some(
                "Fix source-connect and target-connect failures, then rerun Prepare SSH."
                    .to_string(),
            ),
        ));
    }

    result.ok = result.steps.iter().all(|step| step.ok);
    Ok(result)
}

fn is_qemu_disk_key(key: &str) -> bool {
    let prefixes = ["scsi", "virtio", "sata", "ide", "unused"];
    prefixes.iter().any(|prefix| {
        key.strip_prefix(prefix)
            .map(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
            .unwrap_or(false)
    }) || matches!(key, "efidisk0" | "tpmstate0")
}

fn is_replayable_qemu_setting(key: &str) -> bool {
    matches!(
        key,
        "agent"
            | "balloon"
            | "bios"
            | "boot"
            | "bootdisk"
            | "cores"
            | "cpu"
            | "description"
            | "hotplug"
            | "machine"
            | "memory"
            | "name"
            | "net0"
            | "net1"
            | "net2"
            | "net3"
            | "net4"
            | "net5"
            | "net6"
            | "net7"
            | "numa"
            | "onboot"
            | "ostype"
            | "protection"
            | "scsihw"
            | "serial0"
            | "serial1"
            | "serial2"
            | "serial3"
            | "smbios1"
            | "sockets"
            | "startup"
            | "tablet"
            | "tags"
            | "vga"
    )
}

fn normalize_attach_options(options: &str) -> Option<String> {
    let filtered = options
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty() && !entry.starts_with("size="))
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        None
    } else {
        Some(filtered.join(","))
    }
}

fn parse_qemu_config(config: &str, source_vmid: u32) -> Result<ParsedQemuConfig, Error> {
    let expected_prefix = format!("vm-{source_vmid}-");
    let mut parsed = ParsedQemuConfig::default();

    for raw_line in config.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();

        if key == "name" {
            parsed.name = Some(value.to_string());
            continue;
        }

        if is_qemu_disk_key(key) {
            if key.starts_with("unused") {
                continue;
            }

            if value.contains("media=cdrom") {
                continue;
            }

            let (volid, options) = value.split_once(',').unwrap_or((value, ""));
            let basename = volid.rsplit(':').next().unwrap_or(volid).trim();
            if !basename.starts_with(&expected_prefix) {
                continue;
            }

            parsed.disks.push(ParsedQemuDisk {
                key: key.to_string(),
                source_volid: volid.to_string(),
                source_basename: basename.to_string(),
                attach_options: normalize_attach_options(options),
            });
            continue;
        }

        if is_replayable_qemu_setting(key) {
            parsed.settings.push((key.to_string(), value.to_string()));
        }
    }

    if parsed.disks.is_empty() {
        bail!("no recoverable disk configuration found in stored guest config");
    }

    Ok(parsed)
}

fn recover_disk_basename(
    source_basename: &str,
    source_vmid: u32,
    recovery_vmid: u32,
) -> Result<String, Error> {
    let source_prefix = format!("vm-{source_vmid}-");
    let recovery_prefix = format!("vm-{recovery_vmid}-");
    let Some(suffix) = source_basename.strip_prefix(&source_prefix) else {
        bail!("disk basename '{source_basename}' does not match source vmid {source_vmid}");
    };
    Ok(format!("{recovery_prefix}{suffix}"))
}

fn extract_snapshot_suffix(snapshot: &str) -> Result<&str, Error> {
    let (_, suffix) = snapshot
        .rsplit_once('@')
        .context("snapshot is missing '@' separator")?;
    Ok(suffix)
}

fn recovery_storage_id(job_id: &str) -> String {
    let mut storage_id = format!("offsite-{}", sanitize_id(job_id));
    if storage_id.len() > 32 {
        storage_id.truncate(32);
    }
    storage_id
}

fn sanitize_dataset_component(component: &str) -> String {
    component
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn target_dataset_for_source_dataset(target_dataset: &str, source_dataset: &str) -> String {
    let encoded = source_dataset
        .split('/')
        .filter(|component| !component.is_empty())
        .map(sanitize_dataset_component)
        .collect::<Vec<_>>()
        .join("__");
    format!("{target_dataset}/{encoded}")
}

fn legacy_target_dataset_for_source_dataset(target_dataset: &str, source_dataset: &str) -> String {
    let basename = source_dataset
        .rsplit('/')
        .next()
        .unwrap_or(source_dataset)
        .trim();
    format!("{target_dataset}/{basename}")
}

fn source_snapshot_to_target_snapshot_candidates(
    job: &OffsiteReplicationJob,
    source_snapshot: &str,
) -> Result<Vec<String>, Error> {
    let (source_dataset, suffix) = source_snapshot
        .rsplit_once('@')
        .context("snapshot is missing '@' separator")?;

    let mut candidates = Vec::new();
    let target_prefix = format!("{}/", job.target_dataset);
    if source_dataset == job.target_dataset || source_dataset.starts_with(&target_prefix) {
        candidates.push(format!("{source_dataset}@{suffix}"));
    }

    let primary = format!(
        "{}@{suffix}",
        target_dataset_for_source_dataset(&job.target_dataset, source_dataset)
    );
    let legacy = format!(
        "{}@{suffix}",
        legacy_target_dataset_for_source_dataset(&job.target_dataset, source_dataset)
    );

    for candidate in [primary, legacy] {
        if !candidates.iter().any(|entry| entry == &candidate) {
            candidates.push(candidate);
        }
    }

    Ok(candidates)
}

fn resolve_selected_target_snapshot(
    job: &OffsiteReplicationJob,
    source_snapshot: &str,
) -> Result<String, Error> {
    let candidates = source_snapshot_to_target_snapshot_candidates(job, source_snapshot)?;
    let existing = list_existing_target_snapshots(job, &candidates)?;
    candidates
        .into_iter()
        .find(|candidate| existing.contains(candidate))
        .context("selected recovery snapshot could not be resolved on target")
}

fn selected_lineage_prefix_from_target_snapshot(
    job: &OffsiteReplicationJob,
    target_snapshot: &str,
) -> Option<String> {
    let (dataset, _) = target_snapshot.rsplit_once('@')?;
    let relative_dataset = dataset.strip_prefix(&format!("{}/", job.target_dataset))?;
    let (prefix, _) = relative_dataset.rsplit_once("__")?;
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    }
}

fn list_existing_target_snapshots(
    job: &OffsiteReplicationJob,
    snapshots: &[String],
) -> Result<HashSet<String>, Error> {
    if snapshots.is_empty() {
        return Ok(HashSet::new());
    }

    let mut script = String::from("set -eu\n");
    for snapshot in snapshots {
        script.push_str("if zfs list -H -o name ");
        script.push_str(&shell_escape(snapshot));
        script.push_str(" >/dev/null 2>&1; then\n  echo ");
        script.push_str(&shell_escape(snapshot));
        script.push_str("\nfi\n");
    }

    let output = run_ssh_script_checked(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    )?;

    Ok(output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn list_recorded_target_snapshots(job: &OffsiteReplicationJob) -> Result<Vec<String>, Error> {
    let mut snapshots = HashSet::new();

    let mut catalog = load_recovery_point_catalog(&job.id)?;
    let changed = migrate_recovery_point_catalog(job, &mut catalog)?;
    if changed {
        save_recovery_point_catalog(&job.id, &catalog)?;
    }
    for point in catalog.points {
        if point.target_snapshots.is_empty() {
            if let Ok(candidates) =
                source_snapshot_to_target_snapshot_candidates(job, &point.snapshot)
            {
                snapshots.extend(candidates);
            }
        } else {
            snapshots.extend(point.target_snapshots);
        }
    }

    let mut snapshots: Vec<String> = snapshots.into_iter().collect();
    snapshots.sort();
    Ok(snapshots)
}

pub fn purge_target_snapshots_for_job(job: &OffsiteReplicationJob) -> Result<(), Error> {
    validate_runtime_job(job)?;
    let snapshots = list_recorded_target_snapshots(job)?;
    if snapshots.is_empty() {
        return Ok(());
    }

    let mut script = String::from("set -eu\n");
    for snapshot in snapshots {
        script.push_str("if zfs list -H -o name ");
        script.push_str(&shell_escape(&snapshot));
        script.push_str(" >/dev/null 2>&1; then\n");
        script.push_str("  zfs destroy ");
        script.push_str(&shell_escape(&snapshot));
        script.push_str("\nfi\n");
    }

    run_ssh_script_checked(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    )?;

    Ok(())
}

fn remove_local_job_artifacts(job_id: &str) -> Result<(), Error> {
    for file in [
        history_path(job_id),
        recovery_point_catalog_path(job_id),
        lifecycle_state_path(job_id),
        failover_record_path(job_id),
    ] {
        if let Err(err) = std::fs::remove_file(&file) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err).with_context(|| {
                    format!(
                        "failed to remove off-site replication state file '{}'",
                        file.display()
                    )
                });
            }
        }
    }

    let recovery_dir = recovery_config_dir_for_job(job_id);
    if let Err(err) = std::fs::remove_dir_all(&recovery_dir) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err).with_context(|| {
                format!(
                    "failed to remove off-site recovery config directory '{}'",
                    recovery_dir.display()
                )
            });
        }
    }

    Ok(())
}

fn ensure_recovery_snapshot_available(
    job: &OffsiteReplicationJob,
    source_snapshot: &str,
) -> Result<(), Error> {
    for target_snapshot in source_snapshot_to_target_snapshot_candidates(job, source_snapshot)? {
        let check_script = format!("zfs list -H -o name {}", shell_escape(&target_snapshot));
        let output = run_ssh_script(
            &job.target_remote,
            &job.target_node,
            &job.target_user,
            &job.ssh_private_key,
            &check_script,
        )?;

        if output.status.success() {
            return Ok(());
        }
    }

    bail!(
        "selected recovery snapshot '{}' is no longer available on target dataset '{}'",
        source_snapshot,
        job.target_dataset
    );
}

fn build_qemu_failover_script(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailoverRequest,
    parsed: &ParsedQemuConfig,
    target_storage_id: &str,
    selected_lineage_prefix: Option<&str>,
) -> Result<String, Error> {
    let snapshot_suffix = extract_snapshot_suffix(&request.snapshot)?;
    let recovered_name = request
        .recovered_name
        .clone()
        .or_else(|| parsed.name.clone().map(|name| format!("{name}-dr")))
        .unwrap_or_else(|| format!("recovery-{}", request.recovery_vmid));
    let request_source_parent = request.snapshot.split_once('@').and_then(|(dataset, _)| {
        dataset
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
    });

    let mut script = String::from(
        "set -euo pipefail\nSTATUS=1\nCREATED_VM=0\nCREATED_DATASETS_FILE=$(mktemp)\n",
    );
    script.push_str(&format!(
        "cleanup() {{\n  if [ \"$STATUS\" -ne 0 ]; then\n    if [ \"$CREATED_VM\" = \"1\" ]; then\n      qm stop {} >/dev/null 2>&1 || true\n      qm destroy {} --purge 1 >/dev/null 2>&1 || true\n    fi\n    # Track created datasets line-by-line to keep rollback robust for any dataset name shape.\n    if [ -f \"$CREATED_DATASETS_FILE\" ]; then\n      while IFS= read -r dataset; do\n        [ -n \"$dataset\" ] || continue\n        zfs destroy -r \"$dataset\" >/dev/null 2>&1 || true\n      done < \"$CREATED_DATASETS_FILE\"\n    fi\n  fi\n  rm -f \"$CREATED_DATASETS_FILE\"\n}}\ntrap cleanup EXIT\n",
        request.recovery_vmid, request.recovery_vmid
    ));
    script.push_str(&format!(
        "if qm status {} >/dev/null 2>&1; then\n  echo {} >&2\n  exit 1\nfi\n",
        request.recovery_vmid,
        shell_escape(&format!(
            "target VMID {} already exists",
            request.recovery_vmid
        ))
    ));
    script.push_str(&format!(
        "if ! pvesm status --storage {} >/dev/null 2>&1; then\n  pvesm add zfspool {} --pool {} --content images,rootdir --sparse 1\nfi\n",
        shell_escape(target_storage_id),
        shell_escape(target_storage_id),
        shell_escape(&job.target_dataset),
    ));
    script.push_str(&format!(
        "SNAP_SUFFIX={}\nTARGET_DATASET={}\nresolve_recovery_snapshot() {{\n  source_base=\"$1\"\n  occurrence=\"$2\"\n  lineage_prefix=\"${{3:-}}\"\n  legacy=\"$TARGET_DATASET/$source_base@$SNAP_SUFFIX\"\n  if [ -z \"$lineage_prefix\" ] && [ \"$occurrence\" -eq 1 ] && zfs list -H -o name \"$legacy\" >/dev/null 2>&1; then\n    echo \"$legacy\"\n    return 0\n  fi\n\n  matches=\"$(zfs list -H -t snapshot -o name -s creation -r \"$TARGET_DATASET\" 2>/dev/null | while IFS= read -r snap; do\n    case \"$snap\" in\n      *@$SNAP_SUFFIX)\n        ds=\"${{snap%@*}}\"\n        leaf=\"${{ds##*/}}\"\n        if [ -n \"$lineage_prefix\" ]; then\n          expected=\"$lineage_prefix\"\"__\"\"$source_base\"\n          [ \"$leaf\" = \"$expected\" ] || continue\n          printf '%s\\n' \"$snap\"\n          continue\n        fi\n        if [ \"$leaf\" = \"$source_base\" ]; then\n          printf '%s\\n' \"$snap\"\n          continue\n        fi\n        case \"$leaf\" in\n          *__\"$source_base\") printf '%s\\n' \"$snap\" ;;\n        esac\n        ;;\n    esac\n  done)\"\n\n  matches_clean=\"$(printf '%s\\n' \"$matches\" | sed '/^$/d')\"\n  count=\"$(printf '%s\\n' \"$matches_clean\" | sed '/^$/d' | wc -l)\"\n  if [ \"$count\" -ge \"$occurrence\" ]; then\n    printf '%s\\n' \"$matches_clean\" | sed -n \"${{occurrence}}p\"\n    return 0\n  fi\n\n  if [ \"$count\" -eq 0 ]; then\n    echo {} >&2\n  else\n    echo {} >&2\n    printf '%s\\n' \"$matches_clean\" >&2\n  fi\n  return 1\n}}\n",
        shell_escape(snapshot_suffix),
        shell_escape(&job.target_dataset),
        shell_escape("missing target recovery snapshot for selected suffix"),
        shell_escape("ambiguous target recovery snapshots for selected suffix"),
    ));

    let mut basename_totals: HashMap<&str, usize> = HashMap::new();
    for disk in &parsed.disks {
        *basename_totals
            .entry(disk.source_basename.as_str())
            .or_insert(0) += 1;
    }
    let mut basename_seen: HashMap<&str, usize> = HashMap::new();

    for disk in &parsed.disks {
        let basename_key = disk.source_basename.as_str();
        let duplicate_count = basename_totals.get(basename_key).copied().unwrap_or(0);
        let occurrence = basename_seen
            .entry(basename_key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let duplicate = duplicate_count > 1;

        let mut target_basename =
            recover_disk_basename(&disk.source_basename, job.vmid, request.recovery_vmid)?;
        if duplicate {
            target_basename = format!(
                "{}-{}",
                target_basename,
                sanitize_dataset_component(&disk.key)
            );
        }
        let target_dataset = format!("{}/{}", job.target_dataset, target_basename);
        let lineage_hint = if duplicate {
            None
        } else {
            selected_lineage_prefix
        };
        let preferred_target_snapshot = if duplicate {
            None
        } else {
            request_source_parent.as_ref().map(|parent| {
                let dataset = format!("{parent}/{}", disk.source_basename);
                let target_dataset =
                    target_dataset_for_source_dataset(&job.target_dataset, &dataset);
                format!("{target_dataset}@{snapshot_suffix}")
            })
        };
        script.push_str(&format!(
            "zfs list -H -o name {} >/dev/null 2>&1 && {{ echo {} >&2; exit 1; }}\n",
            shell_escape(&target_dataset),
            shell_escape(&format!(
                "target recovery dataset '{}' already exists",
                target_dataset
            ))
        ));
        if let Some(preferred_target_snapshot) = preferred_target_snapshot {
            script.push_str(&format!(
                "if zfs list -H -o name {} >/dev/null 2>&1; then\n  source_snapshot={}\nelse\n  source_snapshot=\"$(resolve_recovery_snapshot {} {} {})\"\nfi\n",
                shell_escape(&preferred_target_snapshot),
                shell_escape(&preferred_target_snapshot),
                shell_escape(&disk.source_basename),
                occurrence,
                shell_escape(lineage_hint.unwrap_or("")),
            ));
        } else {
            script.push_str(&format!(
                "source_snapshot=\"$(resolve_recovery_snapshot {} {} {})\"\n",
                shell_escape(&disk.source_basename),
                occurrence,
                shell_escape(lineage_hint.unwrap_or("")),
            ));
        }
        script.push_str(&format!(
            "printf '%s\\n' {} >> \"$CREATED_DATASETS_FILE\"\n# Clone the replicated snapshot instead of receiving a copy, preserving the shared ZFS lineage required for incremental failback.\nzfs clone {} {}\nif [ \"$(zfs get -H -o value encryption {} 2>/dev/null || echo off)\" != \"off\" ]; then\n  key_status=\"$(zfs get -H -o value keystatus {} 2>/dev/null || echo unavailable)\"\n  if [ \"$key_status\" != \"available\" ]; then\n    echo {} >&2\n    exit 1\n  fi\nfi\n",
            shell_escape(&target_dataset),
            "\"$source_snapshot\"",
            shell_escape(&target_dataset),
            shell_escape(&target_dataset),
            shell_escape(&target_dataset),
            shell_escape(&format!(
                "target recovery dataset '{}' is encrypted but key is unavailable; load key on target before failover",
                target_dataset
            )),
        ));
    }

    script.push_str(&format!(
        "qm create {} --name {}\nCREATED_VM=1\n",
        request.recovery_vmid,
        shell_escape(&recovered_name)
    ));

    let mut delayed_network_settings: Vec<(String, String)> = Vec::new();
    let mut delayed_boot_settings: Vec<(String, String)> = Vec::new();
    for (key, value) in &parsed.settings {
        if key == "name" {
            continue;
        }
        if key.starts_with("net") {
            delayed_network_settings.push((key.clone(), value.clone()));
            continue;
        }
        if key == "boot" || key == "bootdisk" {
            delayed_boot_settings.push((key.clone(), value.clone()));
            continue;
        }
        script.push_str(&format!(
            "qm set {} --{} {}\n",
            request.recovery_vmid,
            key,
            shell_escape(value)
        ));
    }

    for (key, value) in delayed_network_settings {
        // Recovery targets can differ from source network topology. Keep failover deterministic by
        // not replaying source netX settings automatically.
        script.push_str(&format!(
            "echo {} >&2\n",
            shell_escape(&format!(
                "WARN: skipped recovered network setting '{}={}', configure networking manually on the recovery VM",
                key, value
            ))
        ));
    }

    for disk in &parsed.disks {
        let target_basename =
            recover_disk_basename(&disk.source_basename, job.vmid, request.recovery_vmid)?;
        let mut value = format!("{target_storage_id}:{target_basename}");
        if let Some(options) = &disk.attach_options {
            value.push(',');
            value.push_str(options);
        }
        script.push_str(&format!(
            "qm set {} --{} {}\n",
            request.recovery_vmid,
            disk.key,
            shell_escape(&value)
        ));
    }

    for (key, value) in delayed_boot_settings {
        // Boot order can reference devices not replayed in recovery (e.g. netX) and should not
        // abort the entire failover flow when that happens.
        script.push_str(&format!(
            "qm set {} --{} {} >/dev/null 2>&1 || echo {} >&2\n",
            request.recovery_vmid,
            key,
            shell_escape(&value),
            shell_escape(&format!(
                "WARN: skipping incompatible recovered boot setting '{}={}'",
                key, value
            ))
        ));
    }

    if request.start_guest {
        script.push_str(&format!("qm start {}\n", request.recovery_vmid));
    }

    script.push_str("STATUS=0\ntrap - EXIT\n");
    script.push_str(&format!(
        "echo {}\n",
        shell_escape(&format!(
            "prepared recovery VM {} on target node {}",
            request.recovery_vmid, job.target_node
        ))
    ));

    Ok(script)
}

fn execute_failover(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailoverRequest,
) -> Result<String, Error> {
    validate_runtime_job(job)?;
    verify_offsite_snapshot(&request.snapshot)?;
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }

    if job.guest_type != pdm_api_types::resource::GuestType::Qemu {
        bail!("failover is currently implemented only for QEMU guests");
    }

    let stored_config = load_recovery_guest_config(&job.id, &request.snapshot)?;
    let parsed = parse_qemu_config(&stored_config, job.vmid)?;
    let target_storage_id = recovery_storage_id(&job.id);
    let selected_target_snapshot = resolve_selected_target_snapshot(job, &request.snapshot)?;
    let selected_lineage_prefix =
        selected_lineage_prefix_from_target_snapshot(job, &selected_target_snapshot);
    let script = build_qemu_failover_script(
        job,
        request,
        &parsed,
        &target_storage_id,
        selected_lineage_prefix.as_deref(),
    )?;

    run_ssh_script_checked(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    )
}

fn failover_record_for_recovery(
    job_id: &str,
    recovery_vmid: u32,
) -> Result<OffsiteFailoverRecord, Error> {
    list_failover_records(job_id)?
        .into_iter()
        .filter(|record| {
            record.recovery_vmid == recovery_vmid
                && record.lifecycle == OffsiteFailoverLifecycle::Active
        })
        .max_by_key(|record| record.failover_time)
        .with_context(|| {
            format!(
                "no active failover record found for recovery VMID {recovery_vmid} in job '{job_id}'"
            )
        })
}

fn original_source_dataset(
    record: &OffsiteFailoverRecord,
    disk: &ParsedQemuDisk,
) -> Result<String, Error> {
    let (snapshot_dataset, _) = record
        .source_snapshot
        .rsplit_once('@')
        .context("recorded source snapshot is missing '@' separator")?;
    let (parent, _) = snapshot_dataset
        .rsplit_once('/')
        .context("recorded source snapshot dataset has no parent")?;
    Ok(format!("{parent}/{}", disk.source_basename))
}

fn source_restore_dataset(
    record: &OffsiteFailoverRecord,
    disk: &ParsedQemuDisk,
    source_vmid: u32,
    restore_vmid: u32,
) -> Result<String, Error> {
    let source_dataset = original_source_dataset(record, disk)?;
    if source_vmid == restore_vmid {
        return Ok(source_dataset);
    }

    let (parent, _) = source_dataset
        .rsplit_once('/')
        .context("original source dataset has no parent")?;
    Ok(format!(
        "{parent}/{}",
        recover_disk_basename(&disk.source_basename, source_vmid, restore_vmid)?
    ))
}

fn restore_volid(
    disk: &ParsedQemuDisk,
    source_vmid: u32,
    restore_vmid: u32,
) -> Result<String, Error> {
    if source_vmid == restore_vmid {
        return Ok(disk.source_volid.clone());
    }

    let (storage, _) = disk
        .source_volid
        .rsplit_once(':')
        .context("source volume identifier is missing storage")?;
    Ok(format!(
        "{storage}:{}",
        recover_disk_basename(&disk.source_basename, source_vmid, restore_vmid)?
    ))
}

fn matching_disk<'a>(
    disks: &'a [ParsedQemuDisk],
    key: &str,
    description: &str,
) -> Result<&'a ParsedQemuDisk, Error> {
    disks
        .iter()
        .find(|disk| disk.key == key)
        .with_context(|| format!("{description} is missing disk '{key}'"))
}

fn zfs_dataset_from_volid_script(volid: &str) -> String {
    format!(
        "path=$(pvesm path {} 2>/dev/null) || {{ echo {} >&2; exit 1; }}\ncase \"$path\" in\n  /dev/zvol/*) dataset=\"${{path#/dev/zvol/}}\" ;;\n  *) echo {} >&2; exit 1 ;;\nesac\n",
        shell_escape(volid),
        shell_escape(&format!("cannot resolve ZFS dataset for volume '{volid}'")),
        shell_escape(&format!("volume '{volid}' is not backed by a ZFS zvol")),
    )
}

fn reconcile_failover_record(
    job: &OffsiteReplicationJob,
    mut record: OffsiteFailoverRecord,
) -> OffsiteFailoverRecord {
    let checked = proxmox_time::epoch_i64();
    record.last_checked = Some(checked);
    let source_vmid = record.source_restore_vmid.unwrap_or(job.vmid);
    record.source_guest_state = query_guest_state(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        source_vmid,
    )
    .unwrap_or(OffsiteGuestState::Unknown);
    record.target_guest_state = query_guest_state(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        record.recovery_vmid,
    )
    .unwrap_or(OffsiteGuestState::Unknown);

    if !matches!(
        record.lifecycle,
        OffsiteFailoverLifecycle::Active | OffsiteFailoverLifecycle::Superseded
    ) {
        record
            .status_message
            .get_or_insert_with(|| match record.lifecycle {
                OffsiteFailoverLifecycle::Returned => "failback completed".to_string(),
                OffsiteFailoverLifecycle::Abandoned => {
                    "promotion was archived by an operator".to_string()
                }
                OffsiteFailoverLifecycle::Superseded => "promotion was superseded".to_string(),
                OffsiteFailoverLifecycle::Active => String::new(),
            });
        return record;
    }
    if record.target_guest_state == OffsiteGuestState::Missing {
        record.lineage = OffsiteFailbackLineage::Diverged;
        record.status_message = Some(format!(
            "promoted VMID {} is no longer registered on target node '{}'",
            record.recovery_vmid, job.target_node
        ));
        return record;
    }

    let promoted_config = match fetch_qemu_config(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        record.recovery_vmid,
    ) {
        Ok(config) => config,
        Err(err) => {
            record.lineage = OffsiteFailbackLineage::Diverged;
            record.status_message = Some(err.to_string());
            return record;
        }
    };
    let promoted = match parse_qemu_config(&promoted_config, record.recovery_vmid) {
        Ok(config) => config,
        Err(err) => {
            record.lineage = OffsiteFailbackLineage::Diverged;
            record.status_message = Some(err.to_string());
            return record;
        }
    };
    record.current_name = promoted.name.clone();
    let mut script = String::from("set -eu\nlineage=incremental\n");
    for disk in &promoted.disks {
        script.push_str(&zfs_dataset_from_volid_script(&disk.source_volid));
        script.push_str(
            "origin=$(zfs get -H -o value origin \"$dataset\" 2>/dev/null || echo missing)\n\
             if [ \"$origin\" = missing ]; then lineage=diverged; \
             elif [ \"$origin\" = - ] && [ \"$lineage\" = incremental ]; then lineage=full; fi\n",
        );
    }
    script.push_str("printf 'LINEAGE:%s\\n' \"$lineage\"\n");
    match run_ssh_script(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    ) {
        Ok(output) if output.status.success() => {
            let text = String::from_utf8_lossy(&output.stdout);
            record.lineage = if text.lines().any(|line| line == "LINEAGE:incremental") {
                OffsiteFailbackLineage::Incremental
            } else if text.lines().any(|line| line == "LINEAGE:full") {
                OffsiteFailbackLineage::FullRequired
            } else {
                OffsiteFailbackLineage::Diverged
            };
            record.status_message = Some(match record.lineage {
                OffsiteFailbackLineage::Incremental => {
                    "promoted disks retain clone lineage; run precheck before failback".to_string()
                }
                OffsiteFailbackLineage::FullRequired => {
                    "promoted disks have no clone origin; full failback is required".to_string()
                }
                OffsiteFailbackLineage::Diverged => {
                    "one or more promoted disks could not be resolved".to_string()
                }
                OffsiteFailbackLineage::Unchecked => "lineage has not been checked".to_string(),
            });
        }
        Ok(output) => {
            record.lineage = OffsiteFailbackLineage::Diverged;
            record.status_message = Some(merge_command_output(&output));
        }
        Err(err) => {
            record.lineage = OffsiteFailbackLineage::Unchecked;
            record.status_message = Some(err.to_string());
        }
    }
    record
}

pub fn list_reconciled_failover_records(
    job: &OffsiteReplicationJob,
) -> Result<Vec<OffsiteFailoverRecord>, Error> {
    let records: Vec<OffsiteFailoverRecord> = list_failover_records(&job.id)?
        .into_iter()
        .map(|record| reconcile_failover_record(job, record))
        .collect();
    if let Some(active) = records.iter().find(|record| {
        record.lifecycle == OffsiteFailoverLifecycle::Active
            && matches!(
                record.target_guest_state,
                OffsiteGuestState::Running | OffsiteGuestState::Stopped
            )
    }) {
        let lifecycle = load_lifecycle_state(&job.id)?;
        if !lifecycle.suspended {
            // Backfill suspension for promotions created before lifecycle state was persisted.
            suspend_replication_job(&job.id, &active.record_id)?;
        }
    }
    Ok(records)
}

fn failback_precheck_inner(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
) -> Result<OffsiteFailbackPrecheck, Error> {
    validate_runtime_job(job)?;

    let record = failover_record_for_recovery(&job.id, request.recovery_vmid)?;
    let stored_config = load_recovery_guest_config(&job.id, &record.source_snapshot)?;
    let parsed = parse_qemu_config(&stored_config, job.vmid)?;
    let promoted_config = fetch_qemu_config(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        request.recovery_vmid,
    )?;
    let promoted = parse_qemu_config(&promoted_config, request.recovery_vmid)?;
    let base_suffix = extract_snapshot_suffix(&record.source_snapshot)?;
    let (source_host, source_port) = resolve_node_host(&job.source_remote, &job.source_node)?;
    let source_ssh = ssh_target_for_script(&job.source_user, &source_host);
    let source_port_args = ssh_port_args_for_script(source_port);

    let mut script = String::from("set -eu\nincremental=1\n");
    for disk in &parsed.disks {
        let source_dataset = original_source_dataset(&record, disk)?;
        let source_base = format!("{source_dataset}@{base_suffix}");
        let promoted_disk = matching_disk(&promoted.disks, &disk.key, "promoted recovery guest")?;
        script.push_str(&zfs_dataset_from_volid_script(&promoted_disk.source_volid));
        script.push_str(&format!(
            "origin=$(zfs get -H -o value origin \"$dataset\" 2>/dev/null || echo -)\nprintf '%s\\n' \"RECOVERY_ORIGIN:{}:$origin\"\nif [ \"$origin\" = '-' ] || [ \"${{origin##*@}}\" != {} ]; then incremental=0; fi\n",
            shell_escape(&disk.key),
            shell_escape(base_suffix),
        ));
        script.push_str(&format!(
            "ssh -i {} -o BatchMode=yes{} {} -- zfs list -H -o name {} >/dev/null 2>&1 || incremental=0\n",
            shell_escape(&job.ssh_private_key),
            source_port_args,
            source_ssh,
            shell_escape(&source_base),
        ));
        script.push_str(&format!(
            "origin_guid=$(zfs get -H -o value guid \"$origin\" 2>/dev/null || true)\nsource_guid=$(ssh -i {} -o BatchMode=yes{} {} -- zfs get -H -o value guid {} 2>/dev/null || true)\nif [ -z \"$origin_guid\" ] || [ \"$origin_guid\" != \"$source_guid\" ]; then incremental=0; fi\n",
            shell_escape(&job.ssh_private_key),
            source_port_args,
            source_ssh,
            shell_escape(&source_base),
        ));
    }
    script.push_str(&format!(
        "if recovery_status=$(qm status {} 2>/dev/null); then printf '%s\\n' \"RECOVERY_GUEST_STATUS:$recovery_status\"; else echo {} >&2; exit 1; fi\nif source_status=$(ssh -i {} -o BatchMode=yes{} {} -- qm status {} 2>/dev/null); then printf '%s\\n' \"SOURCE_GUEST_STATUS:$source_status\"; else printf '%s\\n' 'SOURCE_GUEST_ABSENT'; fi\nif restore_status=$(ssh -i {} -o BatchMode=yes{} {} -- qm status {} 2>/dev/null); then printf '%s\\n' \"RESTORE_GUEST_STATUS:$restore_status\"; else printf '%s\\n' 'RESTORE_GUEST_ABSENT'; fi\nprintf '%s\\n' \"INCREMENTAL:$incremental\"\n",
        request.recovery_vmid,
        shell_escape(&format!("promoted recovery VMID {} is unavailable", request.recovery_vmid)),
        shell_escape(&job.ssh_private_key),
        source_port_args,
        source_ssh,
        job.vmid,
        shell_escape(&job.ssh_private_key),
        source_port_args,
        source_ssh,
        request.restore_vmid,
    ));

    let output = run_ssh_script(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    )?;

    if output.status.success() {
        let output_text = String::from_utf8_lossy(&output.stdout);
        let source_guest_status = output_text
            .lines()
            .find_map(|line| line.strip_prefix("SOURCE_GUEST_STATUS:"))
            .map(str::trim);
        let source_guest_exists = output_text
            .lines()
            .any(|line| line.starts_with("RESTORE_GUEST_STATUS:"));
        let source_guest_running = source_guest_status
            .map(|status| status.contains("status: running"))
            .unwrap_or(false);
        let recovery_guest_running = output_text
            .lines()
            .find_map(|line| line.strip_prefix("RECOVERY_GUEST_STATUS:"))
            .map(|status| status.contains("status: running"))
            .unwrap_or(false);
        let lineage_incremental = output_text
            .lines()
            .any(|line| line.trim() == "INCREMENTAL:1");
        let incremental = lineage_incremental
            && request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal;
        let message = if request.restore_mode == OffsiteFailbackRestoreMode::RestoreAsNew {
            "restore-as-new uses an explicit full transfer to create independent source datasets"
                .to_string()
        } else if !incremental {
            "promoted guest has no compatible clone lineage; an explicit full failback is required"
                .to_string()
        } else if source_guest_exists {
            format!(
                "incremental failback is available, but source restore VMID {} already exists; choose replacement or another VMID",
                request.restore_vmid,
            )
        } else {
            "incremental failback is available".to_string()
        };
        Ok(OffsiteFailbackPrecheck {
            incremental,
            common_snapshot: incremental.then_some(record.source_snapshot),
            full_required: !incremental,
            source_guest_exists,
            source_guest_running,
            recovery_guest_running,
            suggested_restore_vmid: job.vmid.saturating_add(1_000),
            message,
        })
    } else {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Ok(OffsiteFailbackPrecheck {
            incremental: false,
            common_snapshot: None,
            full_required: true,
            source_guest_exists: false,
            source_guest_running: false,
            recovery_guest_running: false,
            suggested_restore_vmid: job.vmid.saturating_add(1_000),
            message: if message.is_empty() {
                "common failback snapshot is missing; full failback is required".to_string()
            } else {
                message
            },
        })
    }
}

pub fn failback_precheck(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
) -> Result<OffsiteFailbackPrecheck, Error> {
    failback_precheck_inner(job, request)
}

fn build_failback_register_script(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
    parsed: &ParsedQemuConfig,
) -> Result<String, Error> {
    let target_vmid = request.restore_vmid;
    let recovered_name = request
        .recovered_name
        .clone()
        .or_else(|| parsed.name.clone())
        .unwrap_or_else(|| format!("vm-{target_vmid}"));

    let mut script = String::from("set -euo pipefail\n");
    script.push_str(&format!(
        "if qm status {} >/dev/null 2>&1; then\n",
        target_vmid
    ));
    script.push_str(&format!(
        "  echo {} >&2\n  exit 1\n",
        shell_escape(&format!(
            "target VMID {} already exists on original source node",
            target_vmid
        ))
    ));
    script.push_str("fi\n");
    script.push_str(&format!(
        "qm create {} --name {}\n",
        target_vmid,
        shell_escape(&recovered_name)
    ));

    let mut delayed_boot_settings: Vec<(String, String)> = Vec::new();
    for (key, value) in &parsed.settings {
        if key == "name" {
            continue;
        }
        if key == "boot" || key == "bootdisk" {
            delayed_boot_settings.push((key.clone(), value.clone()));
            continue;
        }
        script.push_str(&format!(
            "qm set {} --{} {}\n",
            target_vmid,
            key,
            shell_escape(value)
        ));
    }

    for disk in &parsed.disks {
        let mut value = restore_volid(disk, job.vmid, target_vmid)?;
        if let Some(options) = &disk.attach_options {
            value.push(',');
            value.push_str(options);
        }
        script.push_str(&format!(
            "qm set {} --{} {}\n",
            target_vmid,
            disk.key,
            shell_escape(&value)
        ));
    }

    for (key, value) in delayed_boot_settings {
        script.push_str(&format!(
            "qm set {} --{} {} >/dev/null 2>&1 || echo {} >&2\n",
            target_vmid,
            key,
            shell_escape(&value),
            shell_escape(&format!(
                "WARN: skipping incompatible recovered boot setting '{}={}'",
                key, value
            ))
        ));
    }

    if request.start_guest {
        script.push_str(&format!("qm start {target_vmid}\n"));
    }

    Ok(script)
}

fn failback_staging_dataset(destination_dataset: &str, token: &str) -> Result<String, Error> {
    let (parent, leaf) = destination_dataset
        .rsplit_once('/')
        .context("failback destination dataset has no parent")?;
    Ok(format!("{parent}/pdm-failback-{token}-{leaf}"))
}

fn build_incremental_failback_send_script(
    job: &OffsiteReplicationJob,
    snapshot_tag: &str,
    staging_dataset: &str,
    source_port_args: &str,
    source_ssh: &str,
) -> String {
    format!(
        "origin=$(zfs get -H -o value origin \"$dataset\")\nparent=${{origin%@*}}\nbase=${{origin##*@}}\nrestore_origin() {{\n  if [ \"$(zfs get -H -o value origin \"$parent\")\" != '-' ]; then\n    zfs promote \"$parent\"\n  fi\n}}\nzfs promote \"$dataset\"\ntrap 'restore_origin || true' EXIT\necho {}\nif ! zfs send -w -i \"$dataset@$base\" \"$dataset@{}\" | ssh -i {} -o BatchMode=yes{} {} -- zfs recv -u -F {}; then\n  restore_origin || true\n  trap - EXIT\n  exit 1\nfi\nrestore_origin\ntrap - EXIT\n",
        shell_escape(&format!("incremental failback staging to {staging_dataset}")),
        snapshot_tag,
        shell_escape(&job.ssh_private_key),
        source_port_args,
        source_ssh,
        shell_escape(staging_dataset),
    )
}

fn build_failback_source_prepare_script(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
    record: &OffsiteFailoverRecord,
    parsed: &ParsedQemuConfig,
    incremental: bool,
    token: &str,
) -> Result<String, Error> {
    let target_vmid = request.restore_vmid;
    let mut script = String::from("set -euo pipefail\n");
    script.push_str(&format!(
        "if qm status {} >/dev/null 2>&1; then\n",
        target_vmid
    ));
    if request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal && request.force {
        script.push_str("  :\n");
    } else {
        script.push_str(&format!(
            "  echo {} >&2\n  exit 1\n",
            shell_escape(&format!(
                "target VMID {} already exists on original source node",
                target_vmid
            ))
        ));
    }
    script.push_str("fi\n");
    let base_suffix = extract_snapshot_suffix(&record.source_snapshot)?;
    for disk in &parsed.disks {
        let destination = source_restore_dataset(record, disk, job.vmid, request.restore_vmid)?;
        let staging = failback_staging_dataset(&destination, token)?;
        script.push_str(&format!(
            "if zfs list -H -o name {} >/dev/null 2>&1; then echo {} >&2; exit 1; fi\n",
            shell_escape(&staging),
            shell_escape(&format!(
                "failback staging dataset '{staging}' already exists"
            )),
        ));
        if request.restore_mode == OffsiteFailbackRestoreMode::RestoreAsNew {
            script.push_str(&format!(
                "if zfs list -H -o name {} >/dev/null 2>&1; then echo {} >&2; exit 1; fi\n",
                shell_escape(&destination),
                shell_escape(&format!(
                    "restore destination dataset '{destination}' already exists"
                )),
            ));
        }
        if incremental {
            let source_base = format!("{}@{base_suffix}", original_source_dataset(record, disk)?);
            // A clone cannot accept the incremental receive; seed an independent matching base.
            script.push_str(&format!(
                "zfs list -H -o name {} >/dev/null\nzfs send -w {} | zfs recv -u {}\n",
                shell_escape(&source_base),
                shell_escape(&source_base),
                shell_escape(&staging),
            ));
        }
    }
    Ok(script)
}

fn build_failback_staging_cleanup_script(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
    record: &OffsiteFailoverRecord,
    parsed: &ParsedQemuConfig,
    token: &str,
) -> Result<String, Error> {
    let mut script = String::from("set -eu\n");
    for disk in &parsed.disks {
        let destination = source_restore_dataset(record, disk, job.vmid, request.restore_vmid)?;
        let staging = failback_staging_dataset(&destination, token)?;
        script.push_str(&format!(
            "zfs list -H -o name {} >/dev/null 2>&1 && zfs destroy -r {} || true\n",
            shell_escape(&staging),
            shell_escape(&staging),
        ));
    }
    Ok(script)
}

fn build_failback_cutover_script(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
    record: &OffsiteFailoverRecord,
    parsed: &ParsedQemuConfig,
    token: &str,
) -> Result<String, Error> {
    let mut script = String::from("set -euo pipefail\n");
    if request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal {
        script.push_str(&format!(
            "if qm status {} >/dev/null 2>&1; then\n  qm stop {} >/dev/null 2>&1 || true\n  qm destroy {} --purge 1\nfi\n",
            request.restore_vmid, request.restore_vmid, request.restore_vmid,
        ));
    }
    for disk in &parsed.disks {
        let destination = source_restore_dataset(record, disk, job.vmid, request.restore_vmid)?;
        let staging = failback_staging_dataset(&destination, token)?;
        if request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal {
            script.push_str(&format!(
                "zfs list -H -o name {} >/dev/null 2>&1 && zfs destroy -r {} || true\n",
                shell_escape(&destination),
                shell_escape(&destination),
            ));
        }
        script.push_str(&format!(
            "zfs rename {} {}\n",
            shell_escape(&staging),
            shell_escape(&destination),
        ));
    }
    Ok(script)
}

fn build_full_failback_target_reset_script(
    job: &OffsiteReplicationJob,
    record: &OffsiteFailoverRecord,
    parsed: &ParsedQemuConfig,
) -> Result<String, Error> {
    let mut datasets = HashSet::new();
    if let Some((dataset, _)) = record.target_snapshot.rsplit_once('@') {
        datasets.insert(dataset.to_string());
    }
    for disk in &parsed.disks {
        let source_dataset = original_source_dataset(record, disk)?;
        datasets.insert(target_dataset_for_source_dataset(
            &job.target_dataset,
            &source_dataset,
        ));
        datasets.insert(legacy_target_dataset_for_source_dataset(
            &job.target_dataset,
            &source_dataset,
        ));
    }

    let mut datasets: Vec<String> = datasets.into_iter().collect();
    datasets.sort();
    let mut script = String::new();
    for dataset in datasets {
        let snapshot_prefix = format!("{dataset}@rep_{}_", job.id);
        script.push_str(&format!(
            "if zfs list -H -o name {} >/dev/null 2>&1; then\n  foreign=$(zfs list -H -t snapshot -o name -r {} 2>/dev/null | awk -v prefix={} 'index($0, prefix) != 1 {{ print; exit }}')\n  if [ -n \"$foreign\" ]; then\n    echo {} >&2\n    exit 1\n  fi\n  zfs destroy -r {}\nfi\n",
            shell_escape(&dataset),
            shell_escape(&dataset),
            shell_escape(&snapshot_prefix),
            shell_escape(&format!(
                "cannot reset replication dataset '{dataset}' after full failback because it contains snapshots outside job '{}'",
                job.id
            )),
            shell_escape(&dataset),
        ));
    }
    Ok(script)
}

fn execute_failback(
    job: &OffsiteReplicationJob,
    request: &OffsiteFailbackRequest,
) -> Result<String, Error> {
    validate_runtime_job(job)?;
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }

    if job.guest_type != pdm_api_types::resource::GuestType::Qemu {
        bail!("failback is currently implemented only for QEMU guests");
    }

    let precheck = failback_precheck_inner(job, request)?;
    if !precheck.incremental && !request.allow_full {
        bail!("{}", precheck.message);
    }
    if request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal
        && precheck.source_guest_exists
        && !request.force
    {
        bail!(
            "source VMID {} exists; confirm replacement before failback",
            request.restore_vmid
        );
    }

    let record = failover_record_for_recovery(&job.id, request.recovery_vmid)?;
    let stored_config = load_recovery_guest_config(&job.id, &record.source_snapshot)?;
    let parsed = parse_qemu_config(&stored_config, job.vmid)?;
    let promoted_config = fetch_qemu_config(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        request.recovery_vmid,
    )?;
    let promoted = parse_qemu_config(&promoted_config, request.recovery_vmid)?;
    let (source_host, source_port) = resolve_node_host(&job.source_remote, &job.source_node)?;
    let source_ssh = ssh_target_for_script(&job.source_user, &source_host);
    let source_port_args = ssh_port_args_for_script(source_port);

    let token = format!("{}-{}", sanitize_id(&job.id), proxmox_time::epoch_i64());
    let snapshot_tag = format!("failback_{token}");
    let incremental_transfer =
        precheck.incremental && request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal;
    let prepare_script = build_failback_source_prepare_script(
        job,
        request,
        &record,
        &parsed,
        incremental_transfer,
        &token,
    )?;
    let prepare_output = run_ssh_script_checked(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        &prepare_script,
    )?;

    let mut transfer_script = String::from("set -euo pipefail\n");
    transfer_script.push_str(&format!(
        "if qm status {} 2>/dev/null | grep -q 'status: running'; then\n  qm shutdown {} --timeout 60 >/dev/null 2>&1 || true\n  qm status {} 2>/dev/null | grep -q 'status: running' && qm stop {}\nfi\n",
        request.recovery_vmid, request.recovery_vmid, request.recovery_vmid, request.recovery_vmid,
    ));
    for disk in &parsed.disks {
        let promoted_disk = matching_disk(&promoted.disks, &disk.key, "promoted recovery guest")?;
        let destination_dataset =
            source_restore_dataset(&record, disk, job.vmid, request.restore_vmid)?;
        let staging_dataset = failback_staging_dataset(&destination_dataset, &token)?;

        transfer_script.push_str(&zfs_dataset_from_volid_script(&promoted_disk.source_volid));
        transfer_script.push_str(&format!("zfs snapshot \"$dataset@{snapshot_tag}\"\n",));
        if incremental_transfer {
            transfer_script.push_str(&build_incremental_failback_send_script(
                job,
                &snapshot_tag,
                &staging_dataset,
                &source_port_args,
                &source_ssh,
            ));
        } else {
            transfer_script.push_str(&format!(
                "echo {}\nzfs send -w \"$dataset@{snapshot_tag}\" | ssh -i {} -o BatchMode=yes{} {} -- zfs recv -u -F {}\n",
                shell_escape(&format!("full failback staging to {staging_dataset}")),
                shell_escape(&job.ssh_private_key),
                source_port_args,
                source_ssh,
                shell_escape(&staging_dataset),
            ));
        }
    }

    let transfer_output = match run_ssh_script_checked(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &transfer_script,
    ) {
        Ok(output) => output,
        Err(err) => {
            let cleanup_script =
                build_failback_staging_cleanup_script(job, request, &record, &parsed, &token)?;
            if let Err(cleanup_err) = run_ssh_script_checked(
                &job.source_remote,
                &job.source_node,
                &job.source_user,
                &job.ssh_private_key,
                &cleanup_script,
            ) {
                log::warn!("failed to clean failback staging datasets: {cleanup_err}");
            }
            return Err(err);
        }
    };
    let cutover_script = build_failback_cutover_script(job, request, &record, &parsed, &token)?;
    let cutover_output = run_ssh_script_checked(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        &cutover_script,
    )?;
    let register_script = build_failback_register_script(job, request, &parsed)?;
    let register_output = run_ssh_script_checked(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        &register_script,
    )?;

    let cleanup_output = if request.cleanup_target {
        let mut cleanup_script = format!(
            "set -euo pipefail\nqm destroy {} --purge 1\n",
            request.recovery_vmid
        );
        if !incremental_transfer
            && request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal
        {
            cleanup_script.push_str(&build_full_failback_target_reset_script(
                job, &record, &parsed,
            )?);
        }
        run_ssh_script_checked(
            &job.target_remote,
            &job.target_node,
            &job.target_user,
            &job.ssh_private_key,
            &cleanup_script,
        )?
    } else {
        "kept stopped promoted guest on target for validation".to_string()
    };

    Ok(format!(
        "{prepare_output}\n{transfer_output}\n{cutover_output}\n{register_output}\n{cleanup_output}"
    ))
}

fn validate_runtime_job(job: &OffsiteReplicationJob) -> Result<(), Error> {
    if !PROXMOX_SAFE_ID_REGEX.is_match(&job.id) {
        bail!("job '{}' is invalid: invalid job id", job.id);
    }
    verify_offsite_target_dataset(&job.target_dataset)?;
    verify_offsite_ssh_user(&job.source_user)?;
    verify_offsite_ssh_user(&job.target_user)?;
    verify_offsite_ssh_key_path(&job.ssh_private_key)?;
    if job.source_user != "root" {
        bail!(
            "job '{}' is invalid: source_user must be 'root' for VMID-based replication (current pve-zsync backend requirement)",
            job.id
        );
    }
    if job.max_snapshots == 0 {
        bail!(
            "job '{}' is invalid: max snapshots must be greater than zero",
            job.id
        );
    }
    Ok(())
}

fn build_plain_sync_command(job: &OffsiteReplicationJob, target_host: &str) -> String {
    let source = job.vmid.to_string();
    let dest = format!("{target_host}:{}", job.target_dataset);
    let max_snapshots = job.max_snapshots.to_string();
    let mut cmd = format!(
        "pve-zsync sync --source {} --dest {} --name {} --maxsnap {} --method ssh --source-user {} --dest-user {} --verbose",
        shell_escape(&source),
        shell_escape(&dest),
        shell_escape(&job.id),
        shell_escape(&max_snapshots),
        shell_escape(&job.source_user),
        shell_escape(&job.target_user),
    );

    if let Some(limit_mib) = job.rate_limit_mib {
        let limit_kib = limit_mib.saturating_mul(1024);
        cmd.push_str(&format!(" --limit {limit_kib}"));
    }

    cmd
}

fn build_remote_script(job: &OffsiteReplicationJob, target_host: &str) -> String {
    let mut script = String::from("set -euo pipefail\n");
    script.push_str("FREEZE_DONE=0\n");

    if job.qga_fsfreeze && matches!(job.guest_type, pdm_api_types::resource::GuestType::Qemu) {
        script.push_str(&format!(
            "echo \"QGA fsfreeze: freeze requested\"\nif command -v qm >/dev/null 2>&1; then\n  if FREEZE_OUTPUT=\"$(qm guest cmd {} fsfreeze-freeze 2>&1)\"; then\n    FREEZE_DONE=1\n    echo \"QGA fsfreeze: freeze ok\"\n  else\n    FREEZE_OUTPUT=\"$(printf '%s' \"$FREEZE_OUTPUT\" | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g')\"\n    if [ -n \"$FREEZE_OUTPUT\" ]; then\n      echo \"QGA fsfreeze: freeze skipped ($FREEZE_OUTPUT)\"\n    else\n      echo \"QGA fsfreeze: freeze skipped (command returned non-zero)\"\n    fi\n  fi\nelse\n  echo \"QGA fsfreeze: qm command unavailable\"\nfi\n",
            job.vmid
        ));
    }

    let plain_cmd = build_plain_sync_command(job, target_host);
    let rate_limit_kib = job.rate_limit_mib.map(|value| value * 1024).unwrap_or(0);
    script.push_str(&format!(
        "STREAM_MODE_CONFIG={}\nEFFECTIVE_STREAM_MODE=\"$STREAM_MODE_CONFIG\"\nJOB_ID={}\nVMID={}\nGUEST_TYPE={}\nTARGET_HOST={}\nTARGET_DATASET={}\nSOURCE_USER={}\nTARGET_USER={}\nMAXSNAP={}\nRATE_LIMIT_KIB={}\n",
        shell_escape(job.zfs_stream_mode.as_str()),
        shell_escape(&job.id),
        job.vmid,
        shell_escape(match job.guest_type {
            pdm_api_types::resource::GuestType::Qemu => "qemu",
            pdm_api_types::resource::GuestType::Lxc => "lxc",
        }),
        shell_escape(target_host),
        shell_escape(&job.target_dataset),
        shell_escape(&job.source_user),
        shell_escape(&job.target_user),
        job.max_snapshots,
        rate_limit_kib,
    ));

    script.push_str(
        r#"
collect_volids() {
  if [ "$GUEST_TYPE" = "qemu" ]; then
    qm config "$VMID" --current | awk -F': ' '/^(virtio|ide|scsi|sata|efidisk|tpmstate)[0-9]+: /{print $2}' | cut -d, -f1
  else
    pct config "$VMID" | awk -F': ' '/^(rootfs|mp[0-9]+): /{print $2}' | cut -d, -f1
  fi
}

resolve_dataset_from_volid() {
  volid="$1"
  path="$(pvesm path "$volid" 2>/dev/null || true)"
  [ -n "$path" ] || return 1

  case "$path" in
    /dev/zvol/*)
      echo "${path#/dev/zvol/}"
      return 0
      ;;
  esac

  if zfs list -H -o name "$path" >/dev/null 2>&1; then
    echo "$path"
    return 0
  fi

  case "$path" in
    /*)
      dataset="${path#/}"
      if zfs list -H -o name "$dataset" >/dev/null 2>&1; then
        echo "$dataset"
        return 0
      fi
      ;;
  esac

  return 1
}

collect_source_datasets() {
  collect_volids | while read -r volid; do
    [ -n "$volid" ] || continue
    if dataset="$(resolve_dataset_from_volid "$volid")"; then
      echo "$dataset"
    else
      echo "WARN: skipping volume '$volid' (non-ZFS or unresolved path)" >&2
    fi
  done | awk '!seen[$0]++'
}

target_dataset_for_source() {
  source_ds="$1"
  encoded="$(printf '%s' "$source_ds" | sed 's#/#__#g')"
  echo "$TARGET_DATASET/$encoded"
}

ensure_target_dataset_exists() {
  target_ds="$1"
  ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs list -H -o name "$target_ds" >/dev/null 2>&1 || \
    ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs create -p "$target_ds"
}

last_target_snapshot_for_dataset() {
  target_ds="$1"
  ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- \
    zfs list -H -t snapshot -o name -s creation "$target_ds" 2>/dev/null | \
    awk -v p="${target_ds}@rep_${JOB_ID}_" 'index($0,p)==1 {last=$0} END {print last}'
}

prune_source_snapshots() {
  source_ds="$1"
  snaps="$(zfs list -H -t snapshot -o name -s creation "$source_ds" 2>/dev/null | awk -v p="${source_ds}@rep_${JOB_ID}_" 'index($0,p)==1')"
  count="$(printf '%s\n' "$snaps" | sed '/^$/d' | wc -l)"
  if [ "$count" -gt "$MAXSNAP" ]; then
    trim="$((count - MAXSNAP))"
    printf '%s\n' "$snaps" | sed '/^$/d' | head -n "$trim" | while read -r old; do
      zfs destroy "$old" >/dev/null 2>&1 || echo "WARN: could not destroy source snapshot $old" >&2
    done
  fi
}

prune_target_snapshots() {
  target_ds="$1"
  snaps="$(ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs list -H -t snapshot -o name -s creation "$target_ds" 2>/dev/null | awk -v p="${target_ds}@rep_${JOB_ID}_" 'index($0,p)==1')"
  count="$(printf '%s\n' "$snaps" | sed '/^$/d' | wc -l)"
  if [ "$count" -gt "$MAXSNAP" ]; then
    trim="$((count - MAXSNAP))"
    printf '%s\n' "$snaps" | sed '/^$/d' | head -n "$trim" | while read -r old; do
      ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs destroy "$old" >/dev/null 2>&1 || \
        echo "WARN: could not destroy target snapshot $old" >&2
    done
  fi
}

run_raw_sync() {
  datasets="$1"
  [ -n "$datasets" ] || { echo "ERROR: no ZFS-backed datasets resolved for guest $VMID" >&2; return 1; }

  snap_tag="rep_${JOB_ID}_$(date '+%Y-%m-%d_%H:%M:%S')"
  primary_logged=0
  total_estimated=0

  for source_ds in $datasets; do
    target_ds="$(target_dataset_for_source "$source_ds")"

    new_source_snapshot="${source_ds}@${snap_tag}"
    zfs snapshot "$new_source_snapshot"

    last_target_snapshot="$(last_target_snapshot_for_dataset "$target_ds")"
    transfer_mode="full"
    estimate=""
    if [ -n "$last_target_snapshot" ]; then
      last_tag="${last_target_snapshot##*@}"
      last_source_snapshot="${source_ds}@${last_tag}"
      if zfs list -H -o name "$last_source_snapshot" >/dev/null 2>&1; then
        transfer_mode="incremental"
      fi
    fi

    if [ "$transfer_mode" = "full" ] && ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs list -H -o name "$target_ds" >/dev/null 2>&1; then
      target_snapshot_count="$(ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs list -H -t snapshot -o name "$target_ds" 2>/dev/null | sed '/^$/d' | wc -l)"
      if [ "$target_snapshot_count" -eq 0 ]; then
        echo "WARN: removing stale empty target dataset $target_ds before full raw receive" >&2
        ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs destroy -r "$target_ds"
      else
        echo "ERROR: target dataset $target_ds already exists with snapshots but no matching base for job $JOB_ID" >&2
        return 1
      fi
    fi

    if [ "$transfer_mode" = "incremental" ]; then
      estimate="$(zfs send -nP -w -i "$last_source_snapshot" "$new_source_snapshot" 2>&1 | awk '/size[[:space:]]/ {print $2; exit}')"
      if [ "$RATE_LIMIT_KIB" -gt 0 ]; then
        zfs send -w -i "$last_source_snapshot" "$new_source_snapshot" | cstream -t "$RATE_LIMIT_KIB" | \
          ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs recv -u "$target_ds"
      else
        zfs send -w -i "$last_source_snapshot" "$new_source_snapshot" | \
          ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs recv -u "$target_ds"
      fi
      if [ "$primary_logged" -eq 0 ]; then
        estimate="${estimate:-0}"
        echo "send from $last_source_snapshot to $new_source_snapshot estimated size is ${estimate}B"
        echo "total estimated size is ${estimate}B"
        echo "TIME        SENT   SNAPSHOT $new_source_snapshot"
        echo "00:00:00 ${estimate} ${new_source_snapshot}"
        primary_logged=1
      fi
    else
      estimate="$(zfs send -nP -w "$new_source_snapshot" 2>&1 | awk '/size[[:space:]]/ {print $2; exit}')"
      if [ "$RATE_LIMIT_KIB" -gt 0 ]; then
        zfs send -w "$new_source_snapshot" | cstream -t "$RATE_LIMIT_KIB" | \
          ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs recv -u "$target_ds"
      else
        zfs send -w "$new_source_snapshot" | \
          ssh -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" -- zfs recv -u "$target_ds"
      fi
      if [ "$primary_logged" -eq 0 ]; then
        estimate="${estimate:-0}"
        echo "full send of $new_source_snapshot estimated size is ${estimate}B"
        echo "total estimated size is ${estimate}B"
        echo "TIME        SENT   SNAPSHOT $new_source_snapshot"
        echo "00:00:00 ${estimate} ${new_source_snapshot}"
        primary_logged=1
      fi
    fi

    prune_source_snapshots "$source_ds"
    prune_target_snapshots "$target_ds"
  done
}
"#,
    );

    script.push_str("SOURCE_DATASETS=\"$(collect_source_datasets)\"\n");
    script.push_str("if [ -z \"$SOURCE_DATASETS\" ]; then\n  echo \"ERROR: guest has no ZFS-backed datasets suitable for replication\" >&2\n  exit 1\nfi\n");
    script.push_str("ENCRYPTED_SOURCE=0\nfor source_ds in $SOURCE_DATASETS; do\n  enc=\"$(zfs get -H -o value encryption \"$source_ds\" 2>/dev/null || echo off)\"\n  if [ \"$enc\" != \"off\" ] && [ \"$enc\" != \"-\" ]; then\n    ENCRYPTED_SOURCE=1\n    break\n  fi\ndone\n");
    script.push_str("if [ \"$STREAM_MODE_CONFIG\" = \"auto\" ]; then\n  if [ \"$ENCRYPTED_SOURCE\" = \"1\" ]; then\n    EFFECTIVE_STREAM_MODE=\"raw\"\n  else\n    EFFECTIVE_STREAM_MODE=\"plain\"\n  fi\nfi\n");
    script.push_str("echo \"INFO: zfs-stream configured=$STREAM_MODE_CONFIG effective=$EFFECTIVE_STREAM_MODE source-encrypted=$ENCRYPTED_SOURCE\"\n");

    script.push_str("STATUS=0\n");
    script.push_str("if [ \"$EFFECTIVE_STREAM_MODE\" = \"plain\" ]; then\n");
    script.push_str(&format!("  {} || STATUS=$?\n", plain_cmd));
    script.push_str("else\n");
    script.push_str("  run_raw_sync \"$SOURCE_DATASETS\" || STATUS=$?\n");
    script.push_str("fi\n");

    if job.qga_fsfreeze && matches!(job.guest_type, pdm_api_types::resource::GuestType::Qemu) {
        script.push_str("if [ \"$FREEZE_DONE\" = \"1\" ]; then\n  echo \"QGA fsfreeze: thaw requested\"\n  if THAW_OUTPUT=\"$(qm guest cmd ");
        script.push_str(&format!("{}", job.vmid));
        script.push_str(" fsfreeze-thaw 2>&1)\"; then\n    echo \"QGA fsfreeze: thaw ok\"\n  else\n    THAW_OUTPUT=\"$(printf '%s' \"$THAW_OUTPUT\" | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g')\"\n    if [ -n \"$THAW_OUTPUT\" ]; then\n      echo \"QGA fsfreeze: thaw failed ($THAW_OUTPUT)\"\n    else\n      echo \"QGA fsfreeze: thaw failed\"\n    fi\n  fi\nfi\n");
    }
    script.push_str("exit \"$STATUS\"\n");

    script
}

fn run_option_summary(job: &OffsiteReplicationJob) -> String {
    let rate_limit = job
        .rate_limit_mib
        .map(|limit| format!("{limit} MiB/s"))
        .unwrap_or_else(|| "unlimited".to_string());
    let qga_mode = match (job.qga_fsfreeze, job.guest_type) {
        (true, pdm_api_types::resource::GuestType::Qemu) => "requested",
        (false, pdm_api_types::resource::GuestType::Qemu) => "disabled",
        (true, pdm_api_types::resource::GuestType::Lxc) => "ignored (non-qemu guest)",
        (false, pdm_api_types::resource::GuestType::Lxc) => "not-applicable",
    };
    format!(
        "INFO: options rate-limit={rate_limit}, qga-fsfreeze={qga_mode}, zfs-stream={}, max-snapshots={}, history-limit={}",
        job.zfs_stream_mode.as_str(),
        job.max_snapshots,
        job.history_limit
    )
}

fn run_over_ssh(job: &OffsiteReplicationJob) -> Result<(TaskState, String), Error> {
    validate_runtime_job(job)?;

    let (target_host, _target_port) = resolve_node_host(&job.target_remote, &job.target_node)?;

    let target_host = normalize_host_for_connection(&target_host);
    let script = build_remote_script(job, &target_host);

    let start = Instant::now();
    let output = run_ssh_script(
        &job.source_remote,
        &job.source_node,
        &job.source_user,
        &job.ssh_private_key,
        &script,
    )?;
    let mut merged = merge_command_output(&output);
    if !merged.trim().is_empty() {
        merged.push('\n');
    }
    merged.push_str(&run_option_summary(job));
    merged.push('\n');

    let duration = start.elapsed().as_secs() as i64;
    let endtime = proxmox_time::epoch_i64();
    let starttime = endtime - duration.max(0);

    let mut run = OffsiteReplicationRun {
        start_time: starttime,
        end_time: endtime,
        duration: duration.max(0),
        success: output.status.success(),
        transfer_mode: None,
        source_snapshot: None,
        snapshot: None,
        estimated_bytes: None,
        transferred_bytes: None,
        error: if output.status.success() {
            None
        } else {
            Some(format!(
                "ssh/pve-zsync exited with status {}",
                output
                    .status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            ))
        },
        output: merged.clone(),
    };
    run = annotate_run(run);

    let captured_snapshot = run.snapshot.clone();
    if run.success {
        let config_capture_result = (|| -> Result<(), Error> {
            let snapshot = captured_snapshot
                .as_deref()
                .context("replication succeeded but no recovery snapshot was recorded")?;
            let config = fetch_guest_config(job)?;
            save_recovery_guest_config(&job.id, snapshot, &config)?;
            Ok(())
        })();

        if let Err(err) = config_capture_result {
            run.success = false;
            run.error = Some(format!(
                "replication completed, but persisting recovery config failed: {err}"
            ));
            if !run.output.trim().is_empty() {
                run.output.push('\n');
            }
            run.output.push_str(&format!(
                "WARNING: {}\n",
                run.error.clone().unwrap_or_default()
            ));
        }
    }

    if run.success {
        if let Err(err) = record_recovery_point(job, &run) {
            run.success = false;
            run.error = Some(format!(
                "replication completed, but persisting recovery-point metadata failed: {err}"
            ));
            if !run.output.trim().is_empty() {
                run.output.push('\n');
            }
            run.output.push_str(&format!(
                "WARNING: {}\n",
                run.error.clone().unwrap_or_default()
            ));
        }
    }

    let run_success = run.success;
    let run_snapshot = captured_snapshot.clone();
    let state = if run_success {
        TaskState::OK { endtime }
    } else {
        TaskState::Error {
            endtime,
            message: run
                .error
                .clone()
                .unwrap_or_else(|| "off-site replication failed".to_string()),
        }
    };

    append_history(&job.id, normalize_history_limit(job.history_limit), run)?;

    if !merged.trim().is_empty() && !merged.ends_with('\n') {
        merged.push('\n');
    }
    if run_success {
        if let Some(snapshot) = run_snapshot {
            merged.push_str(&format!(
                "captured guest config for recovery point {snapshot}\n"
            ));
        }
    }

    Ok((state, merged))
}

pub fn list_history(
    job_id: &str,
    limit: Option<usize>,
) -> Result<Vec<OffsiteReplicationRun>, Error> {
    let mut runs = load_history(job_id)?.runs;

    if let Some(limit) = limit {
        if runs.len() > limit {
            let start = runs.len() - limit;
            runs = runs.split_off(start);
        }
    }

    Ok(runs)
}

fn recovery_point_from_run(
    job: &OffsiteReplicationJob,
    run: &OffsiteReplicationRun,
) -> Result<Option<OffsiteRecoveryPoint>, Error> {
    if !run.success {
        return Ok(None);
    }
    let Some(snapshot) = run.snapshot.clone() else {
        return Ok(None);
    };
    let target_snapshots = source_snapshot_to_target_snapshot_candidates(job, &snapshot)?;
    Ok(Some(OffsiteRecoveryPoint {
        snapshot,
        source_snapshot: run.source_snapshot.clone(),
        end_time: run.end_time,
        transfer_mode: run.transfer_mode.clone(),
        estimated_bytes: run.estimated_bytes,
        transferred_bytes: run.transferred_bytes,
        target_snapshots,
    }))
}

fn merge_recovery_point(catalog: &mut RecoveryPointCatalog, point: OffsiteRecoveryPoint) -> bool {
    if let Some(existing) = catalog
        .points
        .iter_mut()
        .find(|existing| existing.snapshot == point.snapshot)
    {
        if existing != &point {
            *existing = point;
            return true;
        }
        return false;
    }
    catalog.points.push(point);
    true
}

fn record_recovery_point(
    job: &OffsiteReplicationJob,
    run: &OffsiteReplicationRun,
) -> Result<(), Error> {
    let Some(point) = recovery_point_from_run(job, run)? else {
        return Ok(());
    };
    let mut catalog = load_recovery_point_catalog(&job.id)?;
    if merge_recovery_point(&mut catalog, point) {
        catalog.points.sort_by_key(|point| point.end_time);
        save_recovery_point_catalog(&job.id, &catalog)?;
    }
    Ok(())
}

fn migrate_recovery_point_catalog(
    job: &OffsiteReplicationJob,
    catalog: &mut RecoveryPointCatalog,
) -> Result<bool, Error> {
    let mut changed = false;
    for run in load_history(&job.id)?.runs {
        if let Some(point) = recovery_point_from_run(job, &run)? {
            if load_recovery_guest_config(&job.id, &point.snapshot).is_ok() {
                changed |= merge_recovery_point(catalog, point);
            }
        }
    }
    for record in list_failover_records(&job.id)? {
        if load_recovery_guest_config(&job.id, &record.source_snapshot).is_err() {
            continue;
        }
        changed |= merge_recovery_point(
            catalog,
            OffsiteRecoveryPoint {
                snapshot: record.source_snapshot.clone(),
                source_snapshot: None,
                end_time: record.failover_time,
                transfer_mode: None,
                estimated_bytes: None,
                transferred_bytes: None,
                target_snapshots: vec![record.target_snapshot],
            },
        );
    }
    Ok(changed)
}

pub fn list_recovery_points(
    job: &OffsiteReplicationJob,
) -> Result<Vec<OffsiteRecoveryPoint>, Error> {
    validate_runtime_job(job)?;

    let mut catalog = load_recovery_point_catalog(&job.id)?;
    let mut catalog_changed = migrate_recovery_point_catalog(job, &mut catalog)?;
    let mut candidates: Vec<(Vec<String>, OffsiteRecoveryPoint)> = Vec::new();
    let mut target_snapshots = Vec::new();
    let mut points = Vec::new();
    for point in catalog.points.iter().rev() {
        if load_recovery_guest_config(&job.id, &point.snapshot).is_err() {
            continue;
        }
        let target_snapshot_candidates = if point.target_snapshots.is_empty() {
            source_snapshot_to_target_snapshot_candidates(job, &point.snapshot)?
        } else {
            point.target_snapshots.clone()
        };
        if target_snapshot_candidates.is_empty() {
            continue;
        }

        target_snapshots.extend(target_snapshot_candidates.iter().cloned());
        candidates.push((target_snapshot_candidates, point.clone()));
    }

    let existing_snapshots = list_existing_target_snapshots(job, &target_snapshots)?;
    for (target_snapshot_candidates, point) in candidates {
        if target_snapshot_candidates
            .iter()
            .any(|target_snapshot| existing_snapshots.contains(target_snapshot))
        {
            points.push(point);
        } else {
            catalog_changed = true;
            catalog
                .points
                .retain(|entry| entry.snapshot != point.snapshot);
            let first_target_snapshot = target_snapshot_candidates
                .first()
                .cloned()
                .unwrap_or_else(|| "-".to_string());
            log::debug!(
                "off-site replication: skipping stale recovery point '{}' for job '{}' (target snapshot '{}' missing)",
                point.snapshot,
                job.id,
                first_target_snapshot
            );
        }
    }
    if catalog_changed {
        catalog.points.sort_by_key(|point| point.end_time);
        save_recovery_point_catalog(&job.id, &catalog)?;
    }
    Ok(points)
}

pub fn delete_recovery_point(
    job: &OffsiteReplicationJob,
    source_snapshot: &str,
) -> Result<(), Error> {
    validate_runtime_job(job)?;
    verify_offsite_snapshot(source_snapshot)?;

    let recoverable_points = list_recovery_points(job)?;
    let recoverable = recoverable_points
        .iter()
        .find(|point| point.snapshot == source_snapshot)
        .cloned();
    let Some(recoverable) = recoverable else {
        bail!(
            "snapshot '{}' is not currently recoverable on target '{}'",
            source_snapshot,
            job.target_dataset
        );
    };

    let candidates = if recoverable.target_snapshots.is_empty() {
        source_snapshot_to_target_snapshot_candidates(job, source_snapshot)?
    } else {
        recoverable.target_snapshots
    };
    let existing = list_existing_target_snapshots(job, &candidates)?;
    let to_destroy: Vec<String> = candidates
        .into_iter()
        .filter(|snapshot| existing.contains(snapshot))
        .collect();
    if to_destroy.is_empty() {
        bail!(
            "snapshot '{}' is no longer present on target '{}'",
            source_snapshot,
            job.target_dataset
        );
    }

    let mut script = String::from("set -eu\n");
    for target_snapshot in &to_destroy {
        script.push_str("zfs destroy ");
        script.push_str(&shell_escape(target_snapshot));
        script.push('\n');
    }
    run_ssh_script_checked(
        &job.target_remote,
        &job.target_node,
        &job.target_user,
        &job.ssh_private_key,
        &script,
    )?;

    let config_path = recovery_config_path(&job.id, source_snapshot);
    if let Err(err) = std::fs::remove_file(&config_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err).with_context(|| {
                format!(
                    "failed to remove recovery config '{}' after snapshot deletion",
                    config_path.display()
                )
            });
        }
    }

    let mut catalog = load_recovery_point_catalog(&job.id)?;
    catalog
        .points
        .retain(|point| point.snapshot != source_snapshot);
    save_recovery_point_catalog(&job.id, &catalog)?;

    Ok(())
}

fn due_now(job: &OffsiteReplicationJob) -> bool {
    if job.disable {
        return false;
    }

    let event = match job.schedule.parse::<CalendarEvent>() {
        Ok(event) => event,
        Err(err) => {
            log::error!(
                "off-site replication: invalid schedule '{}' for job '{}': {err}",
                job.schedule,
                job.id
            );
            return false;
        }
    };

    let last = match jobstate::last_run_time(WORKER_TYPE, &job.id) {
        Ok(last) => last,
        Err(err) => {
            log::warn!(
                "off-site replication: could not determine last run time for '{}': {err}",
                job.id
            );
            return false;
        }
    };

    let next = match event.compute_next_event(last) {
        Ok(Some(next)) => next,
        Ok(None) => return false,
        Err(err) => {
            log::warn!(
                "off-site replication: could not compute next run for '{}': {err}",
                job.id
            );
            return false;
        }
    };

    next <= proxmox_time::epoch_i64()
}

pub fn ensure_job_state(job_id: &str) -> Result<(), Error> {
    jobstate::create_state_file(WORKER_TYPE, job_id)
}

pub fn remove_job_state(job_id: &str) -> Result<(), Error> {
    jobstate::remove_state_file(WORKER_TYPE, job_id)
}

pub fn remove_job_local_artifacts(job_id: &str) -> Result<(), Error> {
    remove_job_state(job_id)?;
    remove_local_job_artifacts(job_id)?;
    Ok(())
}

pub fn runtime_status(job: &OffsiteReplicationJob) -> OffsiteReplicationRuntimeStatus {
    let state = JobState::load(WORKER_TYPE, &job.id).ok();
    let history = load_history(&job.id).ok();

    let mut status = OffsiteReplicationRuntimeStatus::default();
    match load_lifecycle_state(&job.id) {
        Ok(lifecycle) => {
            status.suspended = lifecycle.suspended;
            status.suspend_reason = lifecycle.reason;
        }
        Err(err) => {
            status.suspended = true;
            status.suspend_reason = Some(format!("cannot read lifecycle state: {err}"));
        }
    }
    if status.suspended {
        status.next_run = None;
    }
    if let Some(state) = state {
        match &state {
            JobState::Created { .. } => {}
            JobState::Started { .. } => status.running = true,
            JobState::Finished { state, .. } => {
                if let TaskState::Error { message, .. } = state {
                    status.last_error = Some(message.clone());
                }
            }
        }

        if let Ok(schedule) = crate::jobstate::compute_schedule_status(&state, Some(&job.schedule))
        {
            if !status.suspended {
                status.next_run = schedule.next_run;
            }
            status.last_run = schedule.last_run_endtime.or(schedule
                .last_run_upid
                .as_ref()
                .and_then(|upid| {
                    upid.parse::<pdm_api_types::UPID>()
                        .ok()
                        .map(|parsed| parsed.starttime)
                }));
            if let Some(state) = &schedule.last_run_state {
                if state != "OK" {
                    status.last_error = Some(state.clone());
                }
            }
        }
    }

    if let Some(history) = history {
        status.run_count = Some(history.runs.len() as u64);
        status.failure_count = Some(history.runs.iter().filter(|run| !run.success).count() as u64);

        if let Some(last) = history.runs.last() {
            status.last_run = Some(last.end_time);
            status.last_duration = Some(last.duration);
            status.last_transfer_bytes = last.transferred_bytes;
            status.last_snapshot = last.snapshot.clone();
            if last.success {
                status.last_success = Some(last.end_time);
            } else if status.last_error.is_none() {
                status.last_error = last.error.clone();
            }
        }

        if status.last_success.is_none() {
            status.last_success = history
                .runs
                .iter()
                .rev()
                .find(|run| run.success)
                .map(|run| run.end_time);
        }
    }

    status
}

pub fn to_status(job: OffsiteReplicationJob) -> OffsiteReplicationJobStatus {
    let status = runtime_status(&job);
    OffsiteReplicationJobStatus { job, status }
}

pub fn run_job_now(job: OffsiteReplicationJob, auth_id: &Authid) -> Result<String, Error> {
    validate_runtime_job(&job)?;
    let config_lock = pdm_config::offsite_replication::lock_config()?;
    let lifecycle = load_lifecycle_state(&job.id)?;
    if lifecycle.suspended {
        bail!(
            "replication job '{}' is suspended: {}",
            job.id,
            lifecycle
                .reason
                .as_deref()
                .unwrap_or("an off-site recovery guest is active")
        );
    }

    let mut state = Job::new(WORKER_TYPE, &job.id)?;
    drop(config_lock);
    let worker_id = Some(job.id.clone());
    let auth_id = auth_id.to_string();

    WorkerTask::new_thread(WORKER_TYPE, worker_id, auth_id, false, move |worker| {
        state.start(&worker.upid().to_string())?;
        proxmox_log::info!("starting off-site replication for job '{}'", job.id);

        let (task_state, output) = match run_over_ssh(&job) {
            Ok(result) => result,
            Err(err) => {
                let endtime = proxmox_time::epoch_i64();
                let task_state = TaskState::Error {
                    endtime,
                    message: err.to_string(),
                };
                let failed_run = OffsiteReplicationRun {
                    start_time: endtime,
                    end_time: endtime,
                    duration: 0,
                    success: false,
                    transfer_mode: None,
                    source_snapshot: None,
                    snapshot: None,
                    estimated_bytes: None,
                    transferred_bytes: None,
                    error: Some(err.to_string()),
                    output: run_option_summary(&job),
                };
                if let Err(history_err) = append_history(
                    &job.id,
                    normalize_history_limit(job.history_limit),
                    failed_run,
                ) {
                    log::error!(
                        "failed to persist off-site replication history for '{}': {history_err}",
                        job.id
                    );
                }
                if let Err(state_err) = state.finish(task_state) {
                    log::error!("failed to persist job state for '{}': {state_err}", job.id);
                }
                return Err(err);
            }
        };
        if !output.trim().is_empty() {
            for line in output.lines() {
                println!("{line}");
            }
        }

        let result = match &task_state {
            TaskState::OK { .. } => Ok(()),
            TaskState::Error { message, .. } => Err(anyhow::format_err!("{message}")),
            other => Err(anyhow::format_err!("unexpected task state: {other}")),
        };

        if let Err(err) = state.finish(task_state) {
            log::error!("failed to persist job state for '{}': {err}", job.id);
        }

        result
    })
}

pub fn run_failover_now(
    job: OffsiteReplicationJob,
    request: OffsiteFailoverRequest,
    auth_id: &Authid,
) -> Result<String, Error> {
    validate_runtime_job(&job)?;
    verify_offsite_snapshot(&request.snapshot)?;
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }

    ensure_recovery_snapshot_available(&job, &request.snapshot)?;
    let selected_target_snapshot = resolve_selected_target_snapshot(&job, &request.snapshot)?;

    let config_lock = pdm_config::offsite_replication::lock_config()?;
    let lifecycle = load_lifecycle_state(&job.id)?;
    if lifecycle.suspended {
        bail!(
            "replication job '{}' is already suspended: {}",
            job.id,
            lifecycle
                .reason
                .as_deref()
                .unwrap_or("an off-site recovery operation is active")
        );
    }
    if list_failover_records(&job.id)?
        .iter()
        .any(|record| record.lifecycle == OffsiteFailoverLifecycle::Active)
    {
        bail!("an active promoted guest must be failed back or abandoned first");
    }
    let replication_guard = Job::new(WORKER_TYPE, &job.id)
        .with_context(|| format!("replication job '{}' is currently running", job.id))?;

    let worker_id = Some(format!("{}-{}", job.id, request.recovery_vmid));
    let auth_id = auth_id.to_string();
    let pending_record_id = format!("{}-{}-pending", sanitize_id(&job.id), request.recovery_vmid);
    // Reserve the lifecycle before starting the worker so the scheduler cannot race promotion.
    suspend_replication_job(&job.id, &pending_record_id)?;
    drop(replication_guard);
    drop(config_lock);

    let job_id = job.id.clone();
    let result = WorkerTask::new_thread(
        FAILOVER_WORKER_TYPE,
        worker_id,
        auth_id,
        false,
        move |_worker| {
            proxmox_log::info!(
                "starting off-site failover for job '{}' using recovery point '{}'",
                job.id,
                request.snapshot
            );

            let output = match execute_failover(&job, &request) {
                Ok(output) => output,
                Err(err) => {
                    if let Err(resume_err) = resume_replication_job(&job.id) {
                        log::error!(
                            "failed to release failover lifecycle reservation for '{}': {resume_err}",
                            job.id
                        );
                    }
                    return Err(err);
                }
            };
            let failover_time = proxmox_time::epoch_i64();
            let record_id = format!(
                "{}-{}-{failover_time}",
                sanitize_id(&job.id),
                request.recovery_vmid
            );
            save_failover_record(OffsiteFailoverRecord {
                record_id: record_id.clone(),
                job_id: job.id.clone(),
                source_snapshot: request.snapshot.clone(),
                target_snapshot: selected_target_snapshot.clone(),
                recovery_vmid: request.recovery_vmid,
                recovered_name: request.recovered_name.clone(),
                failover_time,
                failback_time: None,
                source_restore_vmid: None,
                target_cleaned: false,
                lifecycle: OffsiteFailoverLifecycle::Active,
                abandoned_time: None,
                target_guest_state: OffsiteGuestState::Unknown,
                current_name: None,
                source_guest_state: OffsiteGuestState::Unknown,
                lineage: OffsiteFailbackLineage::Unchecked,
                last_checked: None,
                status_message: None,
            })?;
            suspend_replication_job(&job.id, &record_id)?;
            if !output.trim().is_empty() {
                for line in output.lines() {
                    println!("{line}");
                }
            }

            Ok(())
        },
    );

    if result.is_err() {
        resume_replication_job(&job_id)?;
    }
    result
}

pub fn run_failback_now(
    job: OffsiteReplicationJob,
    request: OffsiteFailbackRequest,
    auth_id: &Authid,
) -> Result<String, Error> {
    validate_runtime_job(&job)?;
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }

    let worker_name = format!("{}-{}", job.id, request.recovery_vmid);
    let failback_guard = Job::new(FAILBACK_WORKER_TYPE, &worker_name).with_context(|| {
        format!(
            "failback for recovery VMID {} is already running",
            request.recovery_vmid
        )
    })?;
    let worker_id = Some(worker_name);
    let auth_id = auth_id.to_string();

    WorkerTask::new_thread(
        FAILBACK_WORKER_TYPE,
        worker_id,
        auth_id,
        false,
        move |_worker| {
            let _failback_guard = failback_guard;
            proxmox_log::info!(
                "starting off-site failback for job '{}' using recovery VMID '{}'",
                job.id,
                request.recovery_vmid
            );

            let output = execute_failback(&job, &request)?;
            mark_failback_complete(
                &job.id,
                request.recovery_vmid,
                request.restore_vmid,
                request.cleanup_target,
            )?;
            if !output.trim().is_empty() {
                for line in output.lines() {
                    println!("{line}");
                }
            }

            Ok(())
        },
    )
}

pub fn run_due_jobs() -> Result<(), Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    for job in config.jobs {
        match load_lifecycle_state(&job.id) {
            Ok(state) if state.suspended => continue,
            Ok(_) => {}
            Err(err) => {
                log::error!(
                    "off-site replication: cannot read lifecycle state for '{}': {err}",
                    job.id
                );
                continue;
            }
        }
        if !due_now(&job) {
            continue;
        }

        if let Err(err) = run_job_now(job.clone(), &Authid::root_auth_id()) {
            log::error!(
                "off-site replication: failed to schedule due job '{}': {err}",
                job.id
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pdm_api_types::resource::GuestType;
    use pdm_api_types::OffsiteZfsStreamMode;

    fn sample_job(guest_type: GuestType) -> OffsiteReplicationJob {
        OffsiteReplicationJob {
            id: "job-100".to_string(),
            source_remote: "src".to_string(),
            source_node: "src-node".to_string(),
            guest_type,
            vmid: 100,
            target_remote: "dst".to_string(),
            target_node: "dst-node".to_string(),
            target_dataset: "tank/offsite".to_string(),
            schedule: "*:0/15".to_string(),
            max_snapshots: 8,
            history_limit: 200,
            rate_limit_mib: Some(64),
            zfs_stream_mode: OffsiteZfsStreamMode::Auto,
            source_user: "root".to_string(),
            target_user: "root".to_string(),
            ssh_private_key: "/root/.ssh/id_ed25519".to_string(),
            qga_fsfreeze: true,
            comment: None,
            disable: false,
        }
    }

    fn sample_failover_record() -> OffsiteFailoverRecord {
        OffsiteFailoverRecord {
            record_id: "job-100-500-1".to_string(),
            job_id: "job-100".to_string(),
            source_snapshot: "tank/vmdata/vm-100-disk-0@rep_job-100_2026-04-17_00:33:06"
                .to_string(),
            target_snapshot: "tank/offsite/vm-100-disk-0@rep_job-100_2026-04-17_00:33:06"
                .to_string(),
            recovery_vmid: 500,
            recovered_name: Some("vm100-dr".to_string()),
            failover_time: 1,
            failback_time: None,
            source_restore_vmid: None,
            target_cleaned: false,
            lifecycle: OffsiteFailoverLifecycle::Active,
            abandoned_time: None,
            target_guest_state: OffsiteGuestState::Stopped,
            current_name: Some("vm100-dr".to_string()),
            source_guest_state: OffsiteGuestState::Stopped,
            lineage: OffsiteFailbackLineage::Incremental,
            last_checked: None,
            status_message: None,
        }
    }

    fn sample_parsed_qemu_config() -> ParsedQemuConfig {
        parse_qemu_config("scsi0: lab-zfs:vm-100-disk-0,size=4G\n", 100)
            .expect("config should parse")
    }

    #[test]
    fn test_sanitize_id() {
        assert_eq!(sanitize_id("job-1"), "job-1");
        assert_eq!(sanitize_id("job 1/2"), "job_1_2");
        assert_eq!(sanitize_id("a:b*c"), "a_b_c");
    }

    #[test]
    fn test_build_remote_script_qemu_has_fsfreeze() {
        let job = sample_job(GuestType::Qemu);
        let script = build_remote_script(&job, "192.0.2.10");

        assert!(script.contains("qm guest cmd 100 fsfreeze-freeze"));
        assert!(script.contains("qm guest cmd 100 fsfreeze-thaw"));
        assert!(script.contains("pve-zsync sync --source '100'"));
        assert!(script.contains("--dest '192.0.2.10:tank/offsite'"));
        assert!(script.contains("--name 'job-100'"));
        assert!(script.contains("--limit 65536"));
    }

    #[test]
    fn test_build_remote_script_lxc_has_no_fsfreeze() {
        let job = sample_job(GuestType::Lxc);
        let script = build_remote_script(&job, "192.0.2.10");

        assert!(!script.contains("fsfreeze-freeze"));
        assert!(!script.contains("fsfreeze-thaw"));
        assert!(script.contains("pve-zsync sync --source '100'"));
    }

    #[test]
    fn test_failback_source_prepare_requires_explicit_replacement() {
        let job = sample_job(GuestType::Qemu);
        let request = OffsiteFailbackRequest {
            recovery_vmid: 500,
            restore_vmid: 100,
            restore_mode: OffsiteFailbackRestoreMode::ReplaceOriginal,
            recovered_name: None,
            start_guest: false,
            allow_full: false,
            force: false,
            cleanup_target: false,
        };

        let record = sample_failover_record();
        let parsed = sample_parsed_qemu_config();
        let safe_script =
            build_failback_source_prepare_script(&job, &request, &record, &parsed, true, "test")
                .expect("safe prepare script should build");
        assert!(safe_script.contains("target VMID 100 already exists on original source node"));
        assert!(!safe_script.contains("qm destroy 100 --purge 1"));

        let replace_request = OffsiteFailbackRequest {
            force: true,
            ..request
        };
        let replace_script = build_failback_source_prepare_script(
            &job,
            &replace_request,
            &record,
            &parsed,
            true,
            "test",
        )
        .expect("replacement prepare script should build");
        assert!(replace_script.contains("zfs send -w"));
        assert!(replace_script.contains("zfs recv -u"));
        assert!(!replace_script.contains("zfs clone"));
        assert!(!replace_script.contains("qm destroy 100 --purge 1"));

        let cutover =
            build_failback_cutover_script(&job, &replace_request, &record, &parsed, "test")
                .expect("cutover script should build");
        assert!(!cutover.contains("zfs promote"));
        let destroy = cutover
            .find("qm destroy 100 --purge 1")
            .expect("source VM is replaced during cutover");
        let rename = cutover
            .find("zfs rename")
            .expect("staging is installed during cutover");
        assert!(destroy < rename);
    }

    #[test]
    fn test_incremental_failback_temporarily_promotes_and_restores_clone_lineage() {
        let script = build_incremental_failback_send_script(
            &sample_job(GuestType::Qemu),
            "failback_test",
            "tank/vmdata/pdm-failback-test-vm-100-disk-0",
            " -p 22",
            "root@source.example",
        );

        let promote_recovery = script
            .find("zfs promote \"$dataset\"")
            .expect("recovery dataset is promoted before sending");
        let send = script
            .find("zfs send -w -i \"$dataset@$base\"")
            .expect("normal incremental stream is sent from the promoted dataset");
        let restore_parent = script
            .rfind("restore_origin")
            .expect("original target lineage is restored after sending");
        assert!(promote_recovery < send);
        assert!(send < restore_parent);
        assert!(script.contains("trap 'restore_origin || true' EXIT"));
        assert!(script.contains("zfs promote \"$parent\""));
    }

    #[test]
    fn test_failback_source_prepare_keeps_existing_source_for_restore_as_new() {
        let job = sample_job(GuestType::Qemu);
        let request = OffsiteFailbackRequest {
            recovery_vmid: 500,
            restore_vmid: 1_100,
            restore_mode: OffsiteFailbackRestoreMode::RestoreAsNew,
            recovered_name: None,
            start_guest: false,
            allow_full: true,
            force: true,
            cleanup_target: false,
        };

        let script = build_failback_source_prepare_script(
            &job,
            &request,
            &sample_failover_record(),
            &sample_parsed_qemu_config(),
            false,
            "test",
        )
        .expect("restore-as-new prepare script should build");
        assert!(script.contains("qm status 1100"));
        assert!(!script.contains("qm destroy 1100 --purge 1"));
    }

    #[test]
    fn test_full_failback_stages_data_before_replacing_source() {
        let job = sample_job(GuestType::Qemu);
        let request = OffsiteFailbackRequest {
            recovery_vmid: 500,
            restore_vmid: 100,
            restore_mode: OffsiteFailbackRestoreMode::ReplaceOriginal,
            recovered_name: None,
            start_guest: false,
            allow_full: true,
            force: true,
            cleanup_target: false,
        };
        let record = sample_failover_record();
        let parsed = sample_parsed_qemu_config();

        let prepare = build_failback_source_prepare_script(
            &job,
            &request,
            &record,
            &parsed,
            false,
            "full-test",
        )
        .expect("full failback prepare script should build");
        assert!(prepare.contains("pdm-failback-full-test"));
        assert!(!prepare.contains("qm destroy 100 --purge 1"));

        let cutover = build_failback_cutover_script(&job, &request, &record, &parsed, "full-test")
            .expect("full failback cutover script should build");
        let destroy = cutover
            .find("qm destroy 100 --purge 1")
            .expect("source replacement happens during cutover");
        let rename = cutover
            .find("zfs rename")
            .expect("staged dataset is installed during cutover");
        assert!(destroy < rename);
    }

    #[test]
    fn test_full_failback_target_reset_rejects_foreign_snapshots() {
        let script = build_full_failback_target_reset_script(
            &sample_job(GuestType::Qemu),
            &sample_failover_record(),
            &sample_parsed_qemu_config(),
        )
        .expect("target reset script should build");

        assert!(script.contains("snapshots outside job"));
        assert!(script.contains("@rep_job-100_"));
        assert!(script.contains("zfs destroy -r"));
        assert!(!script.contains("zfs destroy -R"));
    }

    #[test]
    fn test_failover_uses_zfs_clone_to_preserve_incremental_failback_lineage() {
        let job = sample_job(GuestType::Qemu);
        let request = OffsiteFailoverRequest {
            snapshot: "tank/vmdata/vm-100-disk-0@rep_job-100_2026-04-17_00:33:06".to_string(),
            recovery_vmid: 500,
            recovered_name: Some("vm100-dr".to_string()),
            start_guest: false,
        };
        let parsed = parse_qemu_config("scsi0: lab-zfs:vm-100-disk-0,size=4G\n", 100)
            .expect("config should parse");

        let script = build_qemu_failover_script(&job, &request, &parsed, "offsite-job-100", None)
            .expect("script should build");

        assert!(script.contains("zfs clone \"$source_snapshot\""));
        assert!(
            !script.contains("zfs send -w \"$source_snapshot\""),
            "recovery datasets must remain clones rather than independent received copies"
        );
    }

    #[test]
    fn test_reused_recovery_vmid_supersedes_older_active_record() {
        let mut older = sample_failover_record();
        older.failover_time = 10;
        older.record_id = "older".to_string();
        let mut newer = sample_failover_record();
        newer.failover_time = 20;
        newer.record_id = "newer".to_string();
        let mut records = vec![older, newer];

        supersede_reused_recovery_vmids(&mut records);

        assert_eq!(records[0].lifecycle, OffsiteFailoverLifecycle::Superseded);
        assert_eq!(records[1].lifecycle, OffsiteFailoverLifecycle::Active);
    }

    #[test]
    fn test_legacy_failover_record_gets_active_lifecycle_and_stable_id() {
        let raw = r#"{
            "job-id": "job-100",
            "source-snapshot": "tank/vmdata/vm-100-disk-0@rep_job-100_legacy",
            "target-snapshot": "tank/offsite/vm-100-disk-0@rep_job-100_legacy",
            "recovery-vmid": 500,
            "failover-time": 42
        }"#;
        let mut record: OffsiteFailoverRecord =
            serde_json::from_str(raw).expect("legacy record should deserialize");

        normalize_failover_record(&mut record);

        assert_eq!(record.lifecycle, OffsiteFailoverLifecycle::Active);
        assert_eq!(record.record_id, "job-100-500-42");
    }

    #[test]
    fn test_plain_sync_command_shell_escapes_fields() {
        let mut job = sample_job(GuestType::Qemu);
        job.id = "job'100".to_string();
        job.target_dataset = "tank/off'site".to_string();
        job.target_user = "root'user".to_string();

        let command = build_plain_sync_command(&job, "target'host");

        assert!(command.contains("--dest 'target'\"'\"'host:tank/off'\"'\"'site'"));
        assert!(command.contains("--name 'job'\"'\"'100'"));
        assert!(command.contains("--dest-user 'root'\"'\"'user'"));
    }

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("624"), Some(624));
        assert_eq!(parse_byte_size("624B"), Some(624));
        assert_eq!(parse_byte_size("1K"), Some(1024));
        assert_eq!(parse_byte_size("6.61K"), Some(6769));
    }

    #[test]
    fn test_annotate_full_run() {
        let run = annotate_run(OffsiteReplicationRun {
            start_time: 1,
            end_time: 3,
            duration: 2,
            success: true,
            transfer_mode: None,
            source_snapshot: None,
            snapshot: None,
            estimated_bytes: None,
            transferred_bytes: None,
            error: None,
            output: concat!(
                "full send of tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_02:39:35 estimated size is 6.61K\n",
                "total estimated size is 6.61K\n",
                "TIME SENT SNAPSHOT\n",
                "00:00:02 6.61K tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_02:39:35\n"
            )
            .to_string(),
        });

        assert_eq!(run.transfer_mode.as_deref(), Some("full"));
        assert_eq!(
            run.snapshot.as_deref(),
            Some("tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_02:39:35")
        );
        assert_eq!(run.estimated_bytes, Some(6769));
        assert_eq!(run.transferred_bytes, Some(6769));
    }

    #[test]
    fn test_annotate_incremental_run() {
        let run = annotate_run(OffsiteReplicationRun {
            start_time: 1,
            end_time: 3,
            duration: 2,
            success: true,
            transfer_mode: None,
            source_snapshot: None,
            snapshot: None,
            estimated_bytes: None,
            transferred_bytes: None,
            error: None,
            output: concat!(
                "send from tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_03:10:00 to tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_03:15:00 estimated size is 624B\n",
                "total estimated size is 624\n",
                "TIME SENT SNAPSHOT\n",
                "00:00:02 624 tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_03:15:00\n"
            )
            .to_string(),
        });

        assert_eq!(run.transfer_mode.as_deref(), Some("incremental"));
        assert_eq!(
            run.source_snapshot.as_deref(),
            Some("tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_03:10:00")
        );
        assert_eq!(
            run.snapshot.as_deref(),
            Some("tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-25_03:15:00")
        );
        assert_eq!(run.estimated_bytes, Some(624));
        assert_eq!(run.transferred_bytes, Some(624));
    }

    #[test]
    fn test_annotate_run_falls_back_to_estimate_when_transfer_row_missing() {
        let run = annotate_run(OffsiteReplicationRun {
            start_time: 1,
            end_time: 3,
            duration: 2,
            success: true,
            transfer_mode: None,
            source_snapshot: None,
            snapshot: None,
            estimated_bytes: None,
            transferred_bytes: None,
            error: None,
            output: concat!(
                "send from tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-27_00:45:10 to tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-27_00:50:00 estimated size is 624B\n",
                "total estimated size is 624\n",
                "TIME        SENT   SNAPSHOT tank/vmdata/vm-100-disk-0@rep_job-100_2026-03-27_00:50:00\n"
            )
            .to_string(),
        });

        assert_eq!(run.transfer_mode.as_deref(), Some("incremental"));
        assert_eq!(run.estimated_bytes, Some(624));
        assert_eq!(run.transferred_bytes, Some(624));
    }

    #[test]
    fn test_parse_qemu_config_extracts_replayable_settings_and_disks() {
        let parsed = parse_qemu_config(
            concat!(
                "agent: 1\n",
                "boot: order=virtio0\n",
                "cores: 2\n",
                "memory: 2048\n",
                "name: app01\n",
                "net0: virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0\n",
                "virtio0: local-zfs:vm-100-disk-0,cache=writeback,size=32G\n",
                "unused0: local-zfs:vm-100-disk-0\n",
                "ide2: local-zfs:cloudinit\n",
            ),
            100,
        )
        .expect("config should parse");

        assert_eq!(parsed.name.as_deref(), Some("app01"));
        assert!(parsed
            .settings
            .iter()
            .any(|(key, value)| key == "memory" && value == "2048"));
        assert_eq!(
            parsed.disks,
            vec![ParsedQemuDisk {
                key: "virtio0".to_string(),
                source_volid: "local-zfs:vm-100-disk-0".to_string(),
                source_basename: "vm-100-disk-0".to_string(),
                attach_options: Some("cache=writeback".to_string()),
            }]
        );
    }

    #[test]
    fn test_recover_disk_basename_rewrites_vmid() {
        assert_eq!(
            recover_disk_basename("vm-100-disk-0", 100, 500).unwrap(),
            "vm-500-disk-0"
        );
    }

    #[test]
    fn test_recovery_storage_id_is_sanitized_and_bounded() {
        assert_eq!(recovery_storage_id("job-100"), "offsite-job-100");
        assert_eq!(
            recovery_storage_id("job with very long id/that-needs-sanitizing"),
            "offsite-job_with_very_long_id_th"
        );
    }

    #[test]
    fn test_source_snapshot_candidates_include_direct_target_snapshot() {
        let job = OffsiteReplicationJob {
            id: "job-100".to_string(),
            disable: false,
            source_remote: "src".to_string(),
            source_node: "node-a".to_string(),
            guest_type: pdm_api_types::resource::GuestType::Qemu,
            vmid: 100,
            target_remote: "dst".to_string(),
            target_node: "node-b".to_string(),
            target_dataset: "offsite/replica".to_string(),
            schedule: "*:0/5".to_string(),
            max_snapshots: 4,
            history_limit: 200,
            rate_limit_mib: None,
            source_user: "root".to_string(),
            target_user: "root".to_string(),
            ssh_private_key: "/root/.ssh/id_ed25519".to_string(),
            qga_fsfreeze: false,
            comment: None,
            zfs_stream_mode: OffsiteZfsStreamMode::Auto,
        };

        let snapshot =
            "offsite/replica/tank__vmdata__vm-100-disk-0@rep_job-100_2026-04-16_12:00:00";
        let candidates = source_snapshot_to_target_snapshot_candidates(&job, snapshot)
            .expect("candidate derivation should succeed");
        assert_eq!(
            candidates.first().map(String::as_str),
            Some(snapshot),
            "exact target snapshot should be preferred first"
        );
    }

    #[test]
    fn test_selected_lineage_prefix_for_encoded_target_snapshot() {
        let job = OffsiteReplicationJob {
            id: "job-100".to_string(),
            disable: false,
            source_remote: "src".to_string(),
            source_node: "node-a".to_string(),
            guest_type: pdm_api_types::resource::GuestType::Qemu,
            vmid: 100,
            target_remote: "dst".to_string(),
            target_node: "node-b".to_string(),
            target_dataset: "offsite/replica".to_string(),
            schedule: "*:0/5".to_string(),
            max_snapshots: 4,
            history_limit: 200,
            rate_limit_mib: None,
            source_user: "root".to_string(),
            target_user: "root".to_string(),
            ssh_private_key: "/root/.ssh/id_ed25519".to_string(),
            qga_fsfreeze: false,
            comment: None,
            zfs_stream_mode: OffsiteZfsStreamMode::Auto,
        };

        let prefix = selected_lineage_prefix_from_target_snapshot(
            &job,
            "offsite/replica/tank__vmdata__vm-100-disk-0@rep_job-100_2026-04-16_12:00:00",
        );
        assert_eq!(prefix.as_deref(), Some("tank__vmdata"));
    }

    #[test]
    fn test_build_qemu_failover_script_handles_duplicate_source_basenames() {
        let job = OffsiteReplicationJob {
            id: "job-dup".to_string(),
            disable: false,
            source_remote: "src".to_string(),
            source_node: "node-a".to_string(),
            guest_type: pdm_api_types::resource::GuestType::Qemu,
            vmid: 100,
            target_remote: "dst".to_string(),
            target_node: "node-b".to_string(),
            target_dataset: "offsite/replica".to_string(),
            schedule: "*:0/5".to_string(),
            max_snapshots: 4,
            history_limit: 200,
            rate_limit_mib: None,
            source_user: "root".to_string(),
            target_user: "root".to_string(),
            ssh_private_key: "/root/.ssh/id_ed25519".to_string(),
            qga_fsfreeze: false,
            comment: None,
            zfs_stream_mode: OffsiteZfsStreamMode::Auto,
        };
        let request = OffsiteFailoverRequest {
            snapshot: "tank/vmdata/vm-100-disk-0@rep_job-dup_2026-04-17_00:33:06".to_string(),
            recovery_vmid: 500,
            recovered_name: Some("vm100-dr".to_string()),
            start_guest: false,
        };
        let parsed = parse_qemu_config(
            concat!(
                "name: vm100\n",
                "scsi0: lab-zfs:vm-100-disk-0,size=4G\n",
                "scsi1: encpool:vm-100-disk-0,size=1G\n",
            ),
            100,
        )
        .expect("config should parse");

        let script = build_qemu_failover_script(
            &job,
            &request,
            &parsed,
            "offsite-job-dup",
            Some("tank__vmdata"),
        )
        .expect("script should build");

        assert!(
            script.contains("offsite/replica/vm-500-disk-0-scsi0"),
            "first duplicate disk should use deterministic key suffix"
        );
        assert!(
            script.contains("offsite/replica/vm-500-disk-0-scsi1"),
            "second duplicate disk should use deterministic key suffix"
        );
        assert!(
            script.contains("resolve_recovery_snapshot 'vm-100-disk-0' 1 ''"),
            "first duplicate disk should resolve first matching snapshot"
        );
        assert!(
            script.contains("resolve_recovery_snapshot 'vm-100-disk-0' 2 ''"),
            "second duplicate disk should resolve second matching snapshot"
        );
    }
}
