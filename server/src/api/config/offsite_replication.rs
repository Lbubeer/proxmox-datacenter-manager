use anyhow::{bail, Error};

use proxmox_access_control::CachedUserInfo;
use proxmox_router::{http_bail, http_err, Permission, Router, RpcEnvironment, SubdirMap};
use proxmox_schema::api;
use proxmox_sortable_macro::sortable;

use pdm_api_types::{
    Authid, ConfigDigest, OffsiteFailoverRequest, OffsiteRecoveryPoint, OffsiteReplicationJob,
    OffsiteReplicationJobStatus, OffsiteReplicationRun, OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA,
    OFFSITE_REPLICATION_ID_SCHEMA, PRIV_RESOURCE_AUDIT, PRIV_RESOURCE_MANAGE,
};

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_READ_JOB)
    .put(&API_METHOD_UPDATE_JOB)
    .delete(&API_METHOD_DELETE_JOB)
    .subdirs(ITEM_SUBDIRS);

#[sortable]
const ITEM_SUBDIRS: SubdirMap = &sorted!([
    ("failover", &Router::new().post(&API_METHOD_FAILOVER)),
    ("history", &Router::new().get(&API_METHOD_LIST_HISTORY)),
    (
        "recovery-points",
        &Router::new().get(&API_METHOD_LIST_RECOVERY_POINTS)
    ),
    ("run-now", &Router::new().post(&API_METHOD_RUN_NOW)),
]);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_JOBS)
    .post(&API_METHOD_CREATE_JOB)
    .match_all("id", &ITEM_ROUTER);

fn find_job<'a>(jobs: &'a [OffsiteReplicationJob], id: &str) -> Option<&'a OffsiteReplicationJob> {
    jobs.iter().find(|job| job.id == id)
}

fn validate_job(job: &OffsiteReplicationJob) -> Result<(), Error> {
    if job.source_remote == job.target_remote {
        bail!("source and target remotes must differ");
    }
    if job.source_user != "root" {
        bail!("source_user must be 'root' for guest replication jobs");
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

fn check_source_guest_privs(
    rpcenv: &mut dyn RpcEnvironment,
    job: &OffsiteReplicationJob,
    privilege: u64,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv
        .get_auth_id()
        .ok_or_else(|| http_err!(UNAUTHORIZED, "missing auth id"))?
        .parse()?;
    let user_info = CachedUserInfo::new()?;
    let vmid = job.vmid.to_string();
    user_info.check_privs(
        &auth_id,
        &[
            "resource",
            job.source_remote.as_str(),
            "guest",
            vmid.as_str(),
        ],
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
/// Update an existing off-site replication job.
fn update_job(
    id: String,
    job: OffsiteReplicationJob,
    digest: Option<ConfigDigest>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    if id != job.id {
        bail!("path id and payload id must match");
    }
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

    config.jobs.retain(|job| job.id != id);

    pdm_config::offsite_replication::save_config(&config)?;
    crate::offsite_replication::remove_job_state(&id)?;
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
