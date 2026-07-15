use anyhow::{bail, Error};

use proxmox_access_control::CachedUserInfo;
use proxmox_router::{http_bail, http_err, Permission, Router, RpcEnvironment, SubdirMap};
use proxmox_schema::api;
use proxmox_sortable_macro::sortable;

use pdm_api_types::{
    verify_offsite_recovered_name, verify_offsite_snapshot, verify_offsite_ssh_key_path,
    verify_offsite_ssh_user, verify_offsite_target_dataset, Authid, ConfigDigest,
    OffsiteFailbackPrecheck, OffsiteFailbackRequest, OffsiteFailoverRecord, OffsiteFailoverRequest,
    OffsiteRecoveryOperationStatus, OffsiteRecoveryPoint, OffsiteReplicationJob,
    OffsiteReplicationJobStatus, OffsiteReplicationJobUpdater, OffsiteReplicationRun,
    OffsiteSshKeygenRequest, OffsiteSshKeygenResult, OffsiteSshPrepareRequest,
    OffsiteSshPrepareResult,
    OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA, OFFSITE_REPLICATION_ID_SCHEMA,
    OFFSITE_REPLICATION_SNAPSHOT_SCHEMA, PRIV_RESOURCE_AUDIT, PRIV_RESOURCE_MANAGE,
    PROXMOX_SAFE_ID_REGEX,
};

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_READ_JOB)
    .put(&API_METHOD_UPDATE_JOB)
    .delete(&API_METHOD_DELETE_JOB)
    .subdirs(ITEM_SUBDIRS);

#[sortable]
const ITEM_SUBDIRS: SubdirMap = &sorted!([
    ("failback", &Router::new().post(&API_METHOD_FAILBACK)),
    (
        "failback-precheck",
        &Router::new().post(&API_METHOD_FAILBACK_PRECHECK)
    ),
    ("failover", &Router::new().post(&API_METHOD_FAILOVER)),
    (
        "failover-records",
        &Router::new().get(&API_METHOD_LIST_FAILOVER_RECORDS)
    ),
    (
        "failover-record-abandon",
        &Router::new().post(&API_METHOD_ABANDON_FAILOVER_RECORD)
    ),
    ("history", &Router::new().get(&API_METHOD_LIST_HISTORY)),
    (
        "recovery-snapshot",
        &Router::new().delete(&API_METHOD_DELETE_RECOVERY_SNAPSHOT)
    ),
    (
        "recovery-points",
        &Router::new().get(&API_METHOD_LIST_RECOVERY_POINTS)
    ),
    (
        "recovery-operation",
        &Router::new()
            .get(&API_METHOD_READ_RECOVERY_OPERATION)
            .post(&API_METHOD_ACKNOWLEDGE_RECOVERY_OPERATION)
    ),
    (
        "resume",
        &Router::new().post(&API_METHOD_RESUME_REPLICATION)
    ),
    ("run-now", &Router::new().post(&API_METHOD_RUN_NOW)),
]);

pub const SSH_PREPARE_ROUTER: Router = Router::new().post(&API_METHOD_PREPARE_SSH);
pub const SSH_KEYGEN_ROUTER: Router = Router::new().post(&API_METHOD_KEYGEN_SSH);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_JOBS)
    .post(&API_METHOD_CREATE_JOB)
    .match_all("id", &ITEM_ROUTER);

fn find_job<'a>(jobs: &'a [OffsiteReplicationJob], id: &str) -> Option<&'a OffsiteReplicationJob> {
    jobs.iter().find(|job| job.id == id)
}

fn validate_job(job: &OffsiteReplicationJob) -> Result<(), Error> {
    if !PROXMOX_SAFE_ID_REGEX.is_match(&job.id) {
        bail!("invalid job id '{}'", job.id);
    }
    if job.source_remote == job.target_remote {
        bail!("source and target remotes must differ");
    }
    verify_offsite_target_dataset(&job.target_dataset)?;
    verify_offsite_ssh_user(&job.source_user)?;
    verify_offsite_ssh_user(&job.target_user)?;
    verify_offsite_ssh_key_path(&job.ssh_private_key)?;
    if job.source_user != "root" {
        bail!(
            "source_user must be 'root' for VMID-based replication (current pve-zsync backend requirement)"
        );
    }
    if job.schedule.parse::<proxmox_time::CalendarEvent>().is_err() {
        bail!("invalid schedule '{}'", job.schedule);
    }
    if job.max_snapshots == 0 {
        bail!("max snapshots must be greater than zero");
    }
    if job.history_limit == 0 {
        bail!("history limit must be greater than zero");
    }
    Ok(())
}

fn validate_ssh_prepare_request(request: &OffsiteSshPrepareRequest) -> Result<(), Error> {
    verify_offsite_ssh_user(&request.source_user)?;
    verify_offsite_ssh_user(&request.target_user)?;
    verify_offsite_ssh_key_path(&request.ssh_private_key)
}

fn validate_ssh_keygen_request(request: &OffsiteSshKeygenRequest) -> Result<(), Error> {
    verify_offsite_ssh_key_path(&request.ssh_private_key)
}

fn validate_failover_request(request: &OffsiteFailoverRequest) -> Result<(), Error> {
    verify_offsite_snapshot(&request.snapshot)?;
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }
    Ok(())
}

fn validate_failback_request(request: &OffsiteFailbackRequest) -> Result<(), Error> {
    if request.recovery_vmid == 0 {
        bail!("recovery VMID must be greater than zero");
    }
    if request.restore_vmid == 0 {
        bail!("restore VMID must be greater than zero");
    }
    if let Some(name) = request.recovered_name.as_deref() {
        verify_offsite_recovered_name(name)?;
    }
    Ok(())
}

fn check_source_guest_privs(
    rpcenv: &mut dyn RpcEnvironment,
    job: &OffsiteReplicationJob,
    privilege: u64,
) -> Result<(), Error> {
    check_source_guest_privs_for(rpcenv, &job.source_remote, job.vmid, privilege)
}

fn check_source_guest_privs_for(
    rpcenv: &mut dyn RpcEnvironment,
    source_remote: &str,
    vmid: u32,
    privilege: u64,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;
    let user_info = CachedUserInfo::new()?;
    let vmid = vmid.to_string();
    user_info.check_privs(
        &auth_id,
        &["resource", source_remote, "guest", vmid.as_str()],
        privilege,
        false,
    )
}

// Policy: off-site replication and recovery touch target-side datasets/VM state,
// so target-side authorization is enforced on the remote scope.
fn check_target_remote_privs(
    rpcenv: &mut dyn RpcEnvironment,
    remote: &str,
    privilege: u64,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;
    let user_info = CachedUserInfo::new()?;
    user_info.check_privs(&auth_id, &["resource", remote], privilege, false)
}

#[api(
    protected: true,
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: {
        description: "Visible off-site replication jobs with current runtime status.",
        type: Array,
        items: { type: OffsiteReplicationJobStatus },
    },
)]
/// List off-site replication jobs visible to the current user.
fn list_jobs(rpcenv: &mut dyn RpcEnvironment) -> Result<Vec<OffsiteReplicationJobStatus>, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let auth_id: pdm_api_types::Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;
    let user_info = proxmox_access_control::CachedUserInfo::new()?;

    let mut jobs = Vec::new();
    for job in config.jobs {
        let source_vmid = job.vmid.to_string();
        let source_allowed = user_info
            .check_privs(
                &auth_id,
                &[
                    "resource",
                    job.source_remote.as_str(),
                    "guest",
                    source_vmid.as_str(),
                ],
                PRIV_RESOURCE_AUDIT,
                false,
            )
            .is_ok();
        let target_allowed = user_info
            .check_privs(
                &auth_id,
                &["resource", job.target_remote.as_str()],
                PRIV_RESOURCE_AUDIT,
                false,
            )
            .is_ok();

        if source_allowed && target_allowed {
            jobs.push(crate::offsite_replication::to_status(job));
        }
    }

    Ok(jobs)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for creating an off-site replication job.",
        properties: {
            job: {
                type: OffsiteReplicationJob,
                flatten: true,
            },
            digest: {
                type: ConfigDigest,
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Create a new off-site replication job.
fn create_job(
    job: OffsiteReplicationJob,
    digest: Option<ConfigDigest>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    validate_job(&job)?;
    check_source_guest_privs(rpcenv, &job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;

    let _lock = pdm_config::offsite_replication::lock_config()?;
    let (mut config, expected_digest) = pdm_config::offsite_replication::config()?;
    expected_digest.detect_modification(digest.as_ref())?;

    if find_job(&config.jobs, &job.id).is_some() {
        bail!("job '{}' already exists", job.id);
    }

    // A recreated job id should start with a clean local history/config state.
    crate::offsite_replication::remove_job_local_artifacts(&job.id)?;
    config.jobs.push(job.clone());
    pdm_config::offsite_replication::save_config(&config)?;
    crate::offsite_replication::ensure_job_state(&job.id)?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        description: "Parameters for reading an off-site replication job.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: { type: OffsiteReplicationJobStatus },
)]
/// Read a single off-site replication job.
fn read_job(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<OffsiteReplicationJobStatus, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let job = find_job(&config.jobs, &id)
        .ok_or_else(|| http_err!(NOT_FOUND, "job '{id}' does not exist"))?
        .clone();

    let auth_id: pdm_api_types::Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;
    let user_info = proxmox_access_control::CachedUserInfo::new()?;
    let source_vmid = job.vmid.to_string();
    user_info.check_privs(
        &auth_id,
        &[
            "resource",
            job.source_remote.as_str(),
            "guest",
            source_vmid.as_str(),
        ],
        PRIV_RESOURCE_AUDIT,
        false,
    )?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;

    Ok(crate::offsite_replication::to_status(job))
}

#[api(
    protected: true,
    input: {
        description: "Parameters for updating an off-site replication job.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            job: {
                type: OffsiteReplicationJobUpdater,
                flatten: true,
            },
            digest: {
                type: ConfigDigest,
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Update an existing off-site replication job.
fn update_job(
    id: String,
    job: OffsiteReplicationJobUpdater,
    digest: Option<ConfigDigest>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let job = OffsiteReplicationJob {
        id: id.clone(),
        source_remote: job.source_remote,
        source_node: job.source_node,
        guest_type: job.guest_type,
        vmid: job.vmid,
        target_remote: job.target_remote,
        target_node: job.target_node,
        target_dataset: job.target_dataset,
        schedule: job.schedule,
        max_snapshots: job.max_snapshots,
        history_limit: job.history_limit,
        rate_limit_mib: job.rate_limit_mib,
        zfs_stream_mode: job.zfs_stream_mode,
        source_user: job.source_user,
        target_user: job.target_user,
        ssh_private_key: job.ssh_private_key,
        qga_fsfreeze: job.qga_fsfreeze,
        comment: job.comment,
        disable: job.disable,
    };
    validate_job(&job)?;

    let _lock = pdm_config::offsite_replication::lock_config()?;
    let (mut config, expected_digest) = pdm_config::offsite_replication::config()?;
    expected_digest.detect_modification(digest.as_ref())?;

    let Some(position) = config.jobs.iter().position(|entry| entry.id == id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, &config.jobs[position], PRIV_RESOURCE_MANAGE)?;
    check_source_guest_privs(rpcenv, &job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(
        rpcenv,
        &config.jobs[position].target_remote,
        PRIV_RESOURCE_MANAGE,
    )?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;

    crate::offsite_replication::ensure_lifecycle_safe_update(&config.jobs[position], &job)?;
    config.jobs[position] = job.clone();
    pdm_config::offsite_replication::save_config(&config)?;
    crate::offsite_replication::ensure_job_state(&job.id)?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        description: "Parameters for deleting an off-site replication job.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            digest: {
                type: ConfigDigest,
                optional: true,
            },
            "purge-target-snapshots": {
                type: bool,
                optional: true,
                description: "Also remove recorded target snapshots for this job.",
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Delete an off-site replication job.
fn delete_job(
    id: String,
    digest: Option<ConfigDigest>,
    purge_target_snapshots: Option<bool>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = pdm_config::offsite_replication::lock_config()?;
    let (mut config, expected_digest) = pdm_config::offsite_replication::config()?;
    expected_digest.detect_modification(digest.as_ref())?;

    let Some(job) = find_job(&config.jobs, &id).cloned() else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, &job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::ensure_job_can_be_removed(&job)?;

    if purge_target_snapshots.unwrap_or(false) {
        crate::offsite_replication::purge_target_snapshots_for_job(&job)?;
    }

    config.jobs.retain(|job| job.id != id);

    pdm_config::offsite_replication::save_config(&config)?;
    crate::offsite_replication::remove_job_local_artifacts(&id)?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        description: "Parameters for listing off-site replication history.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            limit: {
                schema: OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA,
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: {
        description: "Recorded off-site replication runs for the selected job.",
        type: Array,
        items: { type: OffsiteReplicationRun },
    },
)]
/// List previously recorded runs for an off-site replication job.
fn list_history(
    id: String,
    limit: Option<u64>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<OffsiteReplicationRun>, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_AUDIT)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;
    crate::offsite_replication::list_history(&id, limit.and_then(|v| usize::try_from(v).ok()))
}

#[api(
    protected: true,
    input: {
        description: "Parameters for listing complete off-site recovery points.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: {
        description: "Recorded recovery points that also have persisted guest configuration.",
        type: Array,
        items: { type: OffsiteRecoveryPoint },
    },
)]
/// List complete recovery points for an off-site replication job.
fn list_recovery_points(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<OffsiteRecoveryPoint>, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_AUDIT)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;
    crate::offsite_replication::list_recovery_points(job)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: {
        type: OffsiteRecoveryOperationStatus,
        optional: true,
    },
)]
/// Read the latest durable recovery operation for a job.
fn read_recovery_operation(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Option<OffsiteRecoveryOperationStatus>, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_AUDIT)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;
    crate::offsite_replication::recovery_operation_status(&id)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Mark a successfully reconciled recovery operation complete.
fn acknowledge_recovery_operation(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::acknowledge_recovery_operation(&id)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for deleting a recoverable snapshot on the target.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            snapshot: {
                schema: OFFSITE_REPLICATION_SNAPSHOT_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Delete one recoverable snapshot from target storage.
fn delete_recovery_snapshot(
    id: String,
    snapshot: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    verify_offsite_snapshot(&snapshot)?;
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::delete_recovery_point(job, snapshot.trim())
}

#[api(
    protected: true,
    input: {
        description: "List recorded failovers for an off-site replication job.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: {
        description: "Recorded failover metadata usable for failback.",
        type: Array,
        items: { type: OffsiteFailoverRecord },
    },
)]
/// List failover records available for failback.
fn list_failover_records(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<OffsiteFailoverRecord>, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_AUDIT)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;

    crate::offsite_replication::list_reconciled_failover_records(job)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            "record-id": {
                type: String,
                description: "Stable identifier of the promoted-guest record.",
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Archive an active promoted-guest record without deleting target data.
fn abandon_failover_record(
    id: String,
    record_id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::abandon_failover_record(job, &record_id)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
)]
/// Resume a suspended replication job after verifying that no active promotion remains.
fn resume_replication(id: String, rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };
    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::resume_suspended_replication(job)
}

#[api(
    protected: true,
    input: {
        description: "Check failback transfer lineage and the post-failback protection repair mode.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            request: {
                type: OffsiteFailbackRequest,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_AUDIT, true),
    },
    returns: { type: OffsiteFailbackPrecheck },
)]
/// Check failback lineage, repair classification, and safety before starting a failback worker.
fn failback_precheck(
    id: String,
    request: OffsiteFailbackRequest,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<OffsiteFailbackPrecheck, Error> {
    validate_failback_request(&request)?;
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_AUDIT)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_AUDIT)?;

    crate::offsite_replication::failback_precheck(job, &request)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for starting an off-site replication job immediately.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
    returns: { schema: proxmox_schema::upid::UPID_SCHEMA },
)]
/// Run an off-site replication job immediately.
fn run_now(id: String, rpcenv: &mut dyn RpcEnvironment) -> Result<String, Error> {
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;

    let auth_id: pdm_api_types::Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;

    crate::offsite_replication::run_job_now(job.clone(), &auth_id)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for preparing SSH access for off-site replication setup.",
        properties: {
            request: {
                type: OffsiteSshPrepareRequest,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
    returns: { type: OffsiteSshPrepareResult },
)]
/// Prepare SSH users/keys for off-site replication setup.
fn prepare_ssh(
    request: OffsiteSshPrepareRequest,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<OffsiteSshPrepareResult, Error> {
    validate_ssh_prepare_request(&request)?;
    check_source_guest_privs_for(
        rpcenv,
        &request.source_remote,
        request.vmid,
        PRIV_RESOURCE_MANAGE,
    )?;
    check_target_remote_privs(rpcenv, &request.target_remote, PRIV_RESOURCE_MANAGE)?;
    crate::offsite_replication::prepare_ssh(&request)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for generating an SSH keypair for off-site replication setup.",
        properties: {
            request: {
                type: OffsiteSshKeygenRequest,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
    returns: { type: OffsiteSshKeygenResult },
)]
/// Generate an SSH keypair on the PDM host for off-site replication setup.
fn keygen_ssh(
    request: OffsiteSshKeygenRequest,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<OffsiteSshKeygenResult, Error> {
    validate_ssh_keygen_request(&request)?;
    check_source_guest_privs_for(
        rpcenv,
        &request.source_remote,
        request.vmid,
        PRIV_RESOURCE_MANAGE,
    )?;
    check_target_remote_privs(rpcenv, &request.target_remote, PRIV_RESOURCE_MANAGE)?;

    crate::offsite_replication::generate_ssh_keypair(&request)
}

#[api(
    protected: true,
    input: {
        description: "Parameters for promoting a recovery point on the target remote.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            request: {
                type: OffsiteFailoverRequest,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
    returns: { schema: proxmox_schema::upid::UPID_SCHEMA },
)]
/// Promote a persisted recovery point to a target-side recovery VM.
fn failover(
    id: String,
    request: OffsiteFailoverRequest,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    validate_failover_request(&request)?;
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;

    let auth_id: pdm_api_types::Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;

    crate::offsite_replication::run_failover_now(job.clone(), request, &auth_id)
}

#[api(
    protected: true,
    input: {
        description: "Send a promoted recovery VM back to the original source node.",
        properties: {
            id: { schema: OFFSITE_REPLICATION_ID_SCHEMA },
            request: {
                type: OffsiteFailbackRequest,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["resource"], PRIV_RESOURCE_MANAGE, true),
    },
    returns: { schema: proxmox_schema::upid::UPID_SCHEMA },
)]
/// Fail back a promoted recovery VM to the original source node.
fn failback(
    id: String,
    request: OffsiteFailbackRequest,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    validate_failback_request(&request)?;
    let (config, _) = pdm_config::offsite_replication::config()?;
    let Some(job) = find_job(&config.jobs, &id) else {
        http_bail!(NOT_FOUND, "job '{}' does not exist", id);
    };

    check_source_guest_privs(rpcenv, job, PRIV_RESOURCE_MANAGE)?;
    check_target_remote_privs(rpcenv, &job.target_remote, PRIV_RESOURCE_MANAGE)?;

    let auth_id: pdm_api_types::Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;

    crate::offsite_replication::run_failback_now(job.clone(), request, &auth_id)
}
