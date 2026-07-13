use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use anyhow::{bail, Error};
use gloo_timers::callback::Timeout;
use proxmox_human_byte::HumanByte;
use serde_json::Value;
use yew::{
    html,
    virtual_dom::{Key, VComp, VNode},
};

use pwt::prelude::*;
use pwt::props::ExtractPrimaryKey;
use pwt::state::{Selection, Store};
use pwt::widget::data_table::{DataTable, DataTableColumn, DataTableHeader};
use pwt::widget::form::{Checkbox, Combobox, DisplayField, Field, FormContext};
use pwt::widget::{
    Button, ButtonType, Column, ConfirmDialog, Container, Dialog, Fa, InputPanel, List, ListTile,
    Panel, Row, TabBarItem, TabPanel, Toolbar, Trigger,
};

use proxmox_schema::IntegerSchema;
use proxmox_yew_comp::form::delete_empty_values;
use proxmox_yew_comp::percent_encoding::percent_encode_component;
use proxmox_yew_comp::utils::{format_duration_human, render_epoch_short};
use proxmox_yew_comp::{http_delete, http_get, http_post, http_put, EditWindow, SchemaValidation};
use proxmox_yew_comp::{
    LoadableComponent, LoadableComponentContext, LoadableComponentMaster,
    LoadableComponentScopeExt, LoadableComponentState, RRDGraph, Series,
};

use pdm_api_types::remotes::RemoteType;
use pdm_api_types::resource::GuestType;
use pdm_api_types::{
    OffsiteFailbackLineage, OffsiteFailbackPrecheck, OffsiteFailbackRequest,
    OffsiteFailbackRestoreMode, OffsiteFailoverLifecycle, OffsiteFailoverRecord,
    OffsiteFailoverRequest, OffsiteGuestState, OffsiteRecoveryPoint, OffsiteReplicationJob,
    OffsiteReplicationJobStatus, OffsiteReplicationRun, OffsiteSshKeygenRequest,
    OffsiteSshKeygenResult, OffsiteSshPrepareRequest, OffsiteSshPrepareResult,
    OffsiteZfsStreamMode, OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA, OFFSITE_REPLICATION_ID_SCHEMA,
    OFFSITE_REPLICATION_MAXSNAP_SCHEMA, OFFSITE_REPLICATION_SCHEDULE_SCHEMA,
};

use crate::renderer::status_row;
use crate::widget::{PveGuestSelector, PveNodeSelector, RemoteSelector};

const BASE_URL: &str = "/config/offsite-replication";
const SSH_PREPARE_URL: &str = "/config/offsite-replication-ssh-prepare";
const SSH_KEYGEN_URL: &str = "/config/offsite-replication-ssh-keygen";
const FAILOVER_VMID_OFFSET: u32 = 400;
const TAB_JOBS: &str = "jobs";
const TAB_METRICS: &str = "metrics";
const TAB_FAILOVER: &str = "failover";
const HISTORY_PAGE_SIZE: usize = 200;
const HISTORY_GRAPH_POINTS: usize = 6;
const AUTO_REFRESH_MS: u32 = 30_000;
const DEFAULT_JOB_SCHEDULE: &str = "*:0/5";
const DEFAULT_MAX_SNAPSHOTS: &str = "4";
const DEFAULT_HISTORY_LIMIT: &str = "200";
const DEFAULT_TARGET_DATASET: &str = "offsite/replica";
const DEFAULT_ZFS_STREAM_MODE: &str = "auto";
const DEFAULT_SOURCE_USER: &str = "root";
const DEFAULT_TARGET_USER: &str = "root";
const DEFAULT_SSH_PRIVATE_KEY: &str = "/root/.ssh/pdm-offsite";
const SSH_STEP_CODE_MISSING_PRIVATE_KEY: &str = "missing_private_key";
const RATE_LIMIT_MIB_SCHEMA: proxmox_schema::Schema =
    IntegerSchema::new("Bandwidth limit (MiB/s).")
        .minimum(1)
        .maximum(1024 * 1024)
        .schema();

#[derive(PartialEq, Clone, Properties)]
pub struct OffsiteReplicationPanel {}

impl OffsiteReplicationPanel {
    pub fn new() -> Self {
        yew::props!(Self {})
    }
}

impl From<OffsiteReplicationPanel> for VNode {
    fn from(value: OffsiteReplicationPanel) -> Self {
        VComp::new::<LoadableComponentMaster<OffsiteReplicationPanelComp>>(Rc::new(value), None)
            .into()
    }
}

#[derive(PartialEq)]
pub enum ViewState {
    Create,
    Edit,
    Remove,
    DeleteRecoverySnapshot,
    PickHistoryJob,
    PickFailoverJob,
    PickRunNowJob,
    ConfirmFailback(bool),
    ConfirmAbandonFailover,
}

#[derive(Copy, Clone)]
enum JobPickerTarget {
    History,
    Failover,
    RunNow,
}

pub enum Msg {
    OpenCreate,
    OpenEdit,
    LoadFinished(Vec<OffsiteReplicationJobStatus>),
    MainTabChanged,
    Remove(Key, bool),
    RemovePurgeChanged(bool),
    Reload,
    RefreshActive,
    RunNowActive,
    RunNowById(String),
    RunNowFinished(String, Result<String, Error>),
    OpenRunNowTaskLog,
    DismissRunNowFeedback,
    PrepareSsh(Value),
    PrepareSshFinished(Result<OffsiteSshPrepareResult, Error>),
    GenerateSshKey(Value),
    GenerateSshKeyFinished(
        OffsiteSshPrepareRequest,
        Result<OffsiteSshKeygenResult, Error>,
    ),
    OpenPrepareSshDetails,
    OpenHistory(Key),
    OpenFailover(Key),
    HistoryLoaded(String, usize, Result<Vec<OffsiteReplicationRun>, Error>),
    RecoveryPointsLoaded(String, Result<Vec<OffsiteRecoveryPoint>, Error>),
    FailoverRecordsLoaded(String, Result<Vec<OffsiteFailoverRecord>, Error>),
    RequestFailover(bool),
    TriggerFailover(String, OffsiteFailoverRequest),
    FailoverFinished(Result<String, Error>),
    RequestFailback(bool),
    TriggerFailback(String, OffsiteFailbackRequest),
    FailbackFinished(Result<String, Error>),
    FailbackPrecheck,
    FailbackPrecheckFinished(Result<OffsiteFailbackPrecheck, Error>),
    RequestAbandonFailover,
    AbandonFailover(String, String),
    AbandonFailoverFinished(Result<(), Error>),
    ResumeReplication,
    ResumeReplicationFinished(Result<(), Error>),
    OpenDeleteRecoverySnapshot,
    DeleteRecoverySnapshot(String),
    DeleteRecoverySnapshotFinished(String, String, Result<(), Error>),
    UpdateFailoverSnapshot(String),
    UpdateFailoverVmid(String),
    UpdateFailoverName(String),
    UpdateFailoverStart(bool),
    UpdateFailbackReplaceSource(bool),
    UpdateFailbackRestoreMode(String),
    UpdateFailbackRestoreVmid(String),
    UpdateFailbackAllowFull(bool),
    UpdateFailbackCleanupTarget(bool),
    FailbackRecordSelected,
    ToggleFailoverAdvanced,
    ResetFailoverDefaults,
    JobsFilterChanged(String),
    JobsJobChanged(String),
    HistoryFilterChanged(String),
    HistoryFilterDetailsChanged(String),
    HistoryFilterResultChanged(String),
    HistoryFilterModeChanged(String),
    HistoryFilterRecoverableChanged(String),
    ToggleHistoryFilters,
    ClearHistoryFilters,
    HistoryRowsChanged(String),
    RecoveryFilterChanged(String),
    RecoveryFilterModeChanged(String),
    RecoveryFilterRecoverableChanged(String),
    ToggleRecoveryFilters,
    ClearRecoveryFilters,
    SelectHistoryJob(String),
    SelectFailoverJob(String),
    OpenHistoryJobPicker,
    OpenFailoverJobPicker,
    CloseJobPicker,
    JobPickerFilterChanged(String),
    ApplyHistoryJobPicker,
    ApplyFailoverJobPicker,
    OpenRunNowJobPicker,
    ApplyRunNowJobPicker,
    ToggleAutoRefresh(bool),
    AutoRefreshTick,
}

fn history_run_key(run: &OffsiteReplicationRun) -> Key {
    format!(
        "{}-{}-{}",
        run.start_time,
        run.end_time,
        run.snapshot.as_deref().unwrap_or_default()
    )
    .into()
}

fn failover_record_key(record: &OffsiteFailoverRecord) -> Key {
    if record.record_id.is_empty() {
        format!(
            "{}-{}-{}",
            record.job_id, record.recovery_vmid, record.failover_time
        )
        .into()
    } else {
        record.record_id.clone().into()
    }
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd)]
struct RunModeSummaryRow {
    mode: String,
    ok_count: u64,
    error_count: u64,
}

impl ExtractPrimaryKey for RunModeSummaryRow {
    fn extract_key(&self) -> Key {
        self.mode.clone().into()
    }
}

#[derive(Clone)]
struct FailoverAction {
    job_id: String,
    request: OffsiteFailoverRequest,
}

#[derive(Clone)]
struct FailbackAction {
    job_id: String,
    request: OffsiteFailbackRequest,
}

#[derive(Clone)]
struct RunNowFeedback {
    job_id: String,
    upid: String,
}

pub struct OffsiteReplicationPanelComp {
    state: LoadableComponentState<ViewState>,
    store: Store<OffsiteReplicationJobStatus>,
    selection: Selection,
    main_tab_selection: Selection,
    columns: Rc<Vec<DataTableHeader<OffsiteReplicationJobStatus>>>,
    jobs_filter_text: String,
    jobs_view_store: Store<OffsiteReplicationJobStatus>,
    run_now_submitting: bool,
    run_now_last_task: Option<String>,
    run_now_feedback: Option<RunNowFeedback>,
    history_job_id: Option<String>,
    history_loading: bool,
    history_runs: Vec<OffsiteReplicationRun>,
    history_store: Store<OffsiteReplicationRun>,
    history_filter_text: String,
    history_filter_details_text: String,
    history_filter_result: String,
    history_filter_mode: String,
    history_filter_recoverable: String,
    history_filters_expanded: bool,
    history_view_store: Store<OffsiteReplicationRun>,
    history_selection: Selection,
    history_requested_limit: usize,
    history_rows_choice: String,
    failover_job_id: Option<String>,
    recovery_points_loading: bool,
    recovery_points: Vec<OffsiteRecoveryPoint>,
    recovery_store: Store<OffsiteRecoveryPoint>,
    recovery_selection: Selection,
    failover_records_loading: bool,
    failover_records: Vec<OffsiteFailoverRecord>,
    failover_record_store: Store<OffsiteFailoverRecord>,
    failover_record_selection: Selection,
    failover_record_columns: Rc<Vec<DataTableHeader<OffsiteFailoverRecord>>>,
    recovery_filter_text: String,
    recovery_filter_mode: String,
    recovery_filter_recoverable: String,
    recovery_filters_expanded: bool,
    recovery_view_store: Store<OffsiteRecoveryPoint>,
    recovery_columns: Rc<Vec<DataTableHeader<OffsiteRecoveryPoint>>>,
    failover_snapshot_input: String,
    failover_vmid_input: String,
    failover_name_input: String,
    failover_start_guest: bool,
    failover_form_dirty: bool,
    failover_advanced_expanded: bool,
    failover_running: bool,
    failover_last_task: Option<String>,
    failback_running: bool,
    failback_last_task: Option<String>,
    failback_precheck: Option<OffsiteFailbackPrecheck>,
    failback_replace_source_guest: bool,
    failback_restore_mode: OffsiteFailbackRestoreMode,
    failback_restore_vmid_input: String,
    failback_allow_full: bool,
    failback_cleanup_target: bool,
    recovery_delete_running: bool,
    recovery_delete_candidate: Option<String>,
    job_picker_filter_text: String,
    job_picker_store: Store<OffsiteReplicationJobStatus>,
    job_picker_selection: Selection,
    job_picker_columns: Rc<Vec<DataTableHeader<OffsiteReplicationJobStatus>>>,
    remove_purge_target_snapshots: bool,
    auto_refresh_enabled: bool,
    auto_refresh_timer: Option<Timeout>,
    ssh_prepare_running: bool,
    ssh_prepare_result: Option<OffsiteSshPrepareResult>,
    ssh_prepare_error: Option<String>,
    ssh_prepare_details_expanded: bool,
    ssh_prepare_last_details: String,
    ssh_keygen_running: bool,
    ssh_keygen_error: Option<String>,
    ssh_keygen_overwrite_armed: bool,
}

pwt::impl_deref_mut_property!(
    OffsiteReplicationPanelComp,
    state,
    LoadableComponentState<ViewState>
);

impl OffsiteReplicationPanelComp {
    fn columns() -> Rc<Vec<DataTableHeader<OffsiteReplicationJobStatus>>> {
        Rc::new(vec![
            DataTableColumn::new(tr!("ID"))
                .width("130px")
                .get_property(|item: &OffsiteReplicationJobStatus| item.job.id.as_str())
                .sort_order(true)
                .into(),
            DataTableColumn::new(tr!("Enabled"))
                .width("80px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    if item.job.disable {
                        tr!("No").into()
                    } else {
                        tr!("Yes").into()
                    }
                })
                .into(),
            DataTableColumn::new(tr!("Guest"))
                .width("90px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!("{}:{}", guest_type_text(item.job.guest_type), item.job.vmid).into()
                })
                .into(),
            DataTableColumn::new(tr!("Source"))
                .width("180px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!("{} / {}", item.job.source_remote, item.job.source_node).into()
                })
                .into(),
            DataTableColumn::new(tr!("Target"))
                .width("220px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!(
                        "{} / {} ({})",
                        item.job.target_remote, item.job.target_node, item.job.target_dataset
                    )
                    .into()
                })
                .into(),
            DataTableColumn::new(tr!("Schedule"))
                .width("120px")
                .get_property(|item: &OffsiteReplicationJobStatus| item.job.schedule.as_str())
                .into(),
            DataTableColumn::new(tr!("Stream"))
                .width("90px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    zfs_stream_mode_text(item.job.zfs_stream_mode).into()
                })
                .into(),
            DataTableColumn::new(tr!("Last Success"))
                .width("140px")
                .render(
                    |item: &OffsiteReplicationJobStatus| match item.status.last_success {
                        Some(time) => render_epoch_short(time).into(),
                        None => "-".into(),
                    },
                )
                .into(),
            DataTableColumn::new(tr!("Last Transfer"))
                .width("110px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    match item.status.last_transfer_bytes {
                        Some(bytes) => HumanByte::from(bytes).to_string().into(),
                        None => "-".into(),
                    }
                })
                .into(),
            DataTableColumn::new(tr!("Last Duration"))
                .width("100px")
                .render(
                    |item: &OffsiteReplicationJobStatus| match item.status.last_duration {
                        Some(duration) => format_duration_human(duration as f64).into(),
                        None => "-".into(),
                    },
                )
                .into(),
            DataTableColumn::new(tr!("Runs"))
                .width("70px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    item.status.run_count.unwrap_or_default().to_string().into()
                })
                .into(),
            DataTableColumn::new(tr!("Failures"))
                .width("80px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    item.status
                        .failure_count
                        .unwrap_or_default()
                        .to_string()
                        .into()
                })
                .into(),
            DataTableColumn::new(tr!("Success Rate"))
                .width("110px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    let runs = item.status.run_count.unwrap_or_default();
                    let failed = item.status.failure_count.unwrap_or_default();
                    let success = runs.saturating_sub(failed);
                    if runs == 0 {
                        "-".into()
                    } else {
                        format!("{:.0}%", (success as f64 / runs as f64) * 100.0).into()
                    }
                })
                .into(),
            DataTableColumn::new(tr!("Next Run"))
                .width("140px")
                .render(
                    |item: &OffsiteReplicationJobStatus| match item.status.next_run {
                        Some(time) => render_epoch_short(time).into(),
                        None => "-".into(),
                    },
                )
                .into(),
            DataTableColumn::new(tr!("Status"))
                .flex(3)
                .render(|item: &OffsiteReplicationJobStatus| status_text(item).into())
                .into(),
        ])
    }

    fn job_picker_columns() -> Rc<Vec<DataTableHeader<OffsiteReplicationJobStatus>>> {
        Rc::new(vec![
            DataTableColumn::new(tr!("ID"))
                .width("150px")
                .get_property(|item: &OffsiteReplicationJobStatus| item.job.id.as_str())
                .sort_order(true)
                .into(),
            DataTableColumn::new(tr!("Guest"))
                .width("100px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!("{}:{}", guest_type_text(item.job.guest_type), item.job.vmid).into()
                })
                .into(),
            DataTableColumn::new(tr!("Source"))
                .width("220px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!("{} / {}", item.job.source_remote, item.job.source_node).into()
                })
                .into(),
            DataTableColumn::new(tr!("Target"))
                .width("260px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    format!(
                        "{} / {} ({})",
                        item.job.target_remote, item.job.target_node, item.job.target_dataset
                    )
                    .into()
                })
                .into(),
            DataTableColumn::new(tr!("Schedule"))
                .width("120px")
                .get_property(|item: &OffsiteReplicationJobStatus| item.job.schedule.as_str())
                .into(),
            DataTableColumn::new(tr!("Stream"))
                .width("90px")
                .render(|item: &OffsiteReplicationJobStatus| {
                    zfs_stream_mode_text(item.job.zfs_stream_mode).into()
                })
                .into(),
            DataTableColumn::new(tr!("Status"))
                .flex(2)
                .render(|item: &OffsiteReplicationJobStatus| status_text(item).into())
                .into(),
        ])
    }

    fn recovery_columns() -> Rc<Vec<DataTableHeader<OffsiteRecoveryPoint>>> {
        Rc::new(vec![
            DataTableColumn::new(tr!("Ended"))
                .width("150px")
                .render(|point: &OffsiteRecoveryPoint| render_epoch_short(point.end_time).into())
                .sort_order(true)
                .into(),
            DataTableColumn::new(tr!("Mode"))
                .width("110px")
                .render(|point: &OffsiteRecoveryPoint| {
                    point
                        .transfer_mode
                        .clone()
                        .unwrap_or_else(|| "-".to_string())
                        .into()
                })
                .into(),
            DataTableColumn::new(tr!("Estimated"))
                .width("110px")
                .render(|point: &OffsiteRecoveryPoint| match point.estimated_bytes {
                    Some(bytes) => HumanByte::from(bytes).to_string().into(),
                    None => "-".into(),
                })
                .into(),
            DataTableColumn::new(tr!("Sent"))
                .width("110px")
                .render(|point: &OffsiteRecoveryPoint| {
                    match point.transferred_bytes.or(point.estimated_bytes) {
                        Some(bytes) => HumanByte::from(bytes).to_string().into(),
                        None => "-".into(),
                    }
                })
                .into(),
            DataTableColumn::new(tr!("Snapshot"))
                .flex(3)
                .render(|point: &OffsiteRecoveryPoint| point.snapshot.clone().into())
                .into(),
        ])
    }

    fn failover_record_columns() -> Rc<Vec<DataTableHeader<OffsiteFailoverRecord>>> {
        Rc::new(vec![
            DataTableColumn::new(tr!("Failover Time"))
                .width("150px")
                .render(|record: &OffsiteFailoverRecord| {
                    render_epoch_short(record.failover_time).into()
                })
                .sort_order(true)
                .into(),
            DataTableColumn::new(tr!("Recovery VMID"))
                .width("120px")
                .render(|record: &OffsiteFailoverRecord| record.recovery_vmid.to_string().into())
                .into(),
            DataTableColumn::new(tr!("Recovered Name"))
                .width("160px")
                .render(|record: &OffsiteFailoverRecord| {
                    record
                        .current_name
                        .clone()
                        .or_else(|| record.recovered_name.clone())
                        .unwrap_or_else(|| "-".into())
                        .into()
                })
                .into(),
            DataTableColumn::new(tr!("Lifecycle"))
                .width("110px")
                .render(|record: &OffsiteFailoverRecord| {
                    failover_lifecycle_text(record.lifecycle).into()
                })
                .into(),
            DataTableColumn::new(tr!("Target VM"))
                .width("100px")
                .render(|record: &OffsiteFailoverRecord| {
                    guest_state_text(record.target_guest_state).into()
                })
                .into(),
            DataTableColumn::new(tr!("Source VM"))
                .width("100px")
                .render(|record: &OffsiteFailoverRecord| {
                    guest_state_text(record.source_guest_state).into()
                })
                .into(),
            DataTableColumn::new(tr!("Lineage"))
                .width("120px")
                .render(|record: &OffsiteFailoverRecord| {
                    failback_lineage_text(record.lineage).into()
                })
                .into(),
            DataTableColumn::new(tr!("Target Snapshot"))
                .flex(2)
                .render(|record: &OffsiteFailoverRecord| {
                    snapshot_tail(&record.target_snapshot).into()
                })
                .into(),
            DataTableColumn::new(tr!("Source Snapshot"))
                .flex(2)
                .render(|record: &OffsiteFailoverRecord| {
                    snapshot_tail(&record.source_snapshot).into()
                })
                .into(),
        ])
    }

    fn selected_job(&self) -> Option<OffsiteReplicationJobStatus> {
        let key = self.selection.selected_key()?;
        self.store.read().lookup_record(&key).cloned()
    }

    fn configured_history_limit_for_job(&self, id: &str) -> usize {
        let default_limit = HISTORY_PAGE_SIZE;
        self.store
            .read()
            .lookup_record(&id.to_string().into())
            .map(|job| job.job.history_limit as usize)
            .unwrap_or(default_limit)
            .max(1)
    }

    fn requested_history_limit_for_job(&self, id: &str) -> usize {
        let configured = self.configured_history_limit_for_job(id);
        let choice =
            self.normalized_history_rows_choice_for_limit(configured, &self.history_rows_choice);
        if choice == "all" {
            configured
        } else {
            choice
                .parse::<usize>()
                .ok()
                .unwrap_or(HISTORY_PAGE_SIZE.min(configured))
                .max(1)
                .min(configured)
        }
    }

    fn normalized_history_rows_choice_for_limit(
        &self,
        configured_limit: usize,
        value: &str,
    ) -> String {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized == "all" {
            return "all".to_string();
        }

        let fallback = HISTORY_PAGE_SIZE.min(configured_limit.max(1));
        normalized
            .parse::<usize>()
            .ok()
            .unwrap_or(fallback)
            .max(1)
            .min(configured_limit.max(1))
            .to_string()
    }

    fn history_rows_items_for_limit(&self, configured_limit: usize) -> Vec<yew::AttrValue> {
        let mut values = vec![100usize, 200, 500, 1000]
            .into_iter()
            .filter(|value| *value <= configured_limit)
            .collect::<Vec<_>>();

        if values.is_empty() || values.last().copied() != Some(configured_limit) {
            values.push(configured_limit);
        }

        values.sort_unstable();
        values.dedup();

        let mut items: Vec<yew::AttrValue> = values
            .into_iter()
            .map(|value| value.to_string().into())
            .collect();
        items.push("all".into());
        items
    }

    fn default_job_id(&self) -> Option<String> {
        self.selection
            .selected_key()
            .map(|key| key.to_string())
            .or_else(|| {
                self.store
                    .read()
                    .data()
                    .first()
                    .map(|job| job.job.id.clone())
            })
    }

    fn active_run_job_id(&self) -> Option<String> {
        match self.current_tab().as_str() {
            TAB_METRICS => self
                .history_job_id
                .clone()
                .or_else(|| self.default_job_id()),
            TAB_FAILOVER => self
                .failover_job_id
                .clone()
                .or_else(|| self.default_job_id()),
            _ => self
                .selection
                .selected_key()
                .map(|key| key.to_string())
                .or_else(|| self.default_job_id()),
        }
    }

    fn schedule_auto_refresh(&mut self, ctx: &LoadableComponentContext<Self>) {
        if !self.auto_refresh_enabled || self.auto_refresh_timer.is_some() {
            return;
        }

        let link = ctx.link().clone();
        self.auto_refresh_timer = Some(Timeout::new(AUTO_REFRESH_MS, move || {
            link.send_message(Msg::AutoRefreshTick);
        }));
    }

    fn refresh_active_tab(&mut self, ctx: &LoadableComponentContext<Self>) {
        match self.current_tab().as_str() {
            TAB_METRICS => {
                if let Some(id) = self
                    .history_job_id
                    .clone()
                    .or_else(|| self.default_job_id())
                {
                    let request_limit = self.requested_history_limit_for_job(&id);
                    self.load_history_for_job_id(id, request_limit, false, ctx);
                }
            }
            TAB_FAILOVER => {
                if let Some(id) = self
                    .failover_job_id
                    .clone()
                    .or_else(|| self.default_job_id())
                {
                    self.load_failover_for_job_id(id, ctx);
                }
            }
            _ => ctx.link().send_reload(),
        }
    }

    fn current_tab(&self) -> String {
        self.main_tab_selection
            .selected_key()
            .map(|key| key.to_string())
            .unwrap_or_else(|| TAB_JOBS.to_string())
    }

    fn ensure_table_selection(&self) {
        if self.selection.selected_key().is_none() {
            if let Some(id) = self
                .store
                .read()
                .data()
                .first()
                .map(|job| job.job.id.clone())
            {
                self.selection.select(id);
            }
        }
    }

    fn normalize_selected_job_ids(&mut self) {
        let ids: HashSet<String> = self
            .store
            .read()
            .data()
            .iter()
            .map(|job| job.job.id.clone())
            .collect();

        let selected_invalid = self
            .selection
            .selected_key()
            .map(|key| !ids.contains(&key.to_string()))
            .unwrap_or(false);
        if selected_invalid {
            if let Some(id) = self
                .store
                .read()
                .data()
                .first()
                .map(|job| job.job.id.clone())
            {
                self.selection.select(id);
            }
        }

        if self
            .history_job_id
            .as_ref()
            .map(|id| !ids.contains(id))
            .unwrap_or(false)
        {
            self.history_job_id = None;
            self.history_runs.clear();
            self.history_store.set_data(Vec::new());
        }

        if self
            .failover_job_id
            .as_ref()
            .map(|id| !ids.contains(id))
            .unwrap_or(false)
        {
            self.failover_job_id = None;
            self.recovery_points.clear();
            self.recovery_store.set_data(Vec::new());
            self.failover_records.clear();
            self.failover_record_store.set_data(Vec::new());
        }
    }

    fn sync_jobs_view_store(&mut self) {
        let filter = self.jobs_filter_text.trim().to_ascii_lowercase();
        let store = self.store.read();
        let data = store.data();
        let filtered: Vec<OffsiteReplicationJobStatus> = if filter.is_empty() {
            data.to_vec()
        } else {
            data.iter()
                .filter(|job| job_matches_filter(job, &filter))
                .cloned()
                .collect()
        };
        self.jobs_view_store.set_data(filtered);
    }

    fn sync_job_picker_store(&mut self) {
        let filter = self.job_picker_filter_text.trim().to_ascii_lowercase();
        let store = self.store.read();
        let data = store.data();
        let filtered: Vec<OffsiteReplicationJobStatus> = if filter.is_empty() {
            data.to_vec()
        } else {
            data.iter()
                .filter(|job| job_matches_filter(job, &filter))
                .cloned()
                .collect()
        };
        self.job_picker_store.set_data(filtered);
    }

    fn history_filters_active_count(&self) -> usize {
        let mut count = 0usize;
        if self.history_filter_result != "all" {
            count += 1;
        }
        if self.history_filter_mode != "all" {
            count += 1;
        }
        if self.history_filter_recoverable != "all" {
            count += 1;
        }
        if !self.history_filter_text.trim().is_empty() {
            count += 1;
        }
        if !self.history_filter_details_text.trim().is_empty() {
            count += 1;
        }
        count
    }

    fn recovery_filters_active_count(&self) -> usize {
        let mut count = 0usize;
        if self.recovery_filter_mode != "all" {
            count += 1;
        }
        if self.recovery_filter_recoverable != "all" {
            count += 1;
        }
        if !self.recovery_filter_text.trim().is_empty() {
            count += 1;
        }
        count
    }

    fn sync_history_view_store(&mut self) {
        let snapshot_filter = self.history_filter_text.trim().to_ascii_lowercase();
        let details_filter = self.history_filter_details_text.trim().to_ascii_lowercase();
        let result_filter = self.history_filter_result.as_str();
        let mode_filter = self.history_filter_mode.as_str();
        let recoverable_filter = self.history_filter_recoverable.as_str();
        let recoverable_set = self.recoverable_snapshot_set();

        let filtered: Vec<OffsiteReplicationRun> = self
            .history_runs
            .iter()
            .filter(|run| {
                if result_filter == "ok" && !run.success {
                    return false;
                }
                if result_filter == "error" && run.success {
                    return false;
                }

                if mode_filter != "all" {
                    let mode = run
                        .transfer_mode
                        .as_deref()
                        .unwrap_or("unknown")
                        .to_ascii_lowercase();
                    if mode != mode_filter {
                        return false;
                    }
                }

                let is_recoverable = run
                    .snapshot
                    .as_deref()
                    .map(|snapshot| recoverable_set.contains(snapshot))
                    .unwrap_or(false);
                if recoverable_filter == "yes" && !is_recoverable {
                    return false;
                }
                if recoverable_filter == "no" && is_recoverable {
                    return false;
                }

                if !snapshot_filter.is_empty() {
                    let snapshot = run.snapshot.as_deref().unwrap_or("").to_ascii_lowercase();
                    if !snapshot.contains(&snapshot_filter) {
                        return false;
                    }
                }

                if !details_filter.is_empty() {
                    let detail = run.error.as_deref().unwrap_or(run.output.as_str());
                    if !detail.to_ascii_lowercase().contains(&details_filter) {
                        return false;
                    }
                }

                true
            })
            .cloned()
            .collect();
        self.history_view_store.set_data(filtered);
    }

    fn sync_recovery_view_store(&mut self) {
        let snapshot_filter = self.recovery_filter_text.trim().to_ascii_lowercase();
        let mode_filter = self.recovery_filter_mode.as_str();
        let recoverable_filter = self.recovery_filter_recoverable.as_str();
        let recoverable_set = self.recoverable_snapshot_set();

        let filtered: Vec<OffsiteRecoveryPoint> = self
            .recovery_points
            .iter()
            .filter(|point| {
                if mode_filter != "all" {
                    let mode = point
                        .transfer_mode
                        .as_deref()
                        .unwrap_or("unknown")
                        .to_ascii_lowercase();
                    if mode != mode_filter {
                        return false;
                    }
                }

                let is_recoverable = recoverable_set.contains(point.snapshot.as_str());
                if recoverable_filter == "yes" && !is_recoverable {
                    return false;
                }
                if recoverable_filter == "no" && is_recoverable {
                    return false;
                }

                if !snapshot_filter.is_empty() {
                    let haystack = point.snapshot.to_ascii_lowercase();
                    if !haystack.contains(&snapshot_filter) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        self.recovery_view_store.set_data(filtered);
    }

    fn load_history_for_job_id(
        &mut self,
        id: String,
        limit: usize,
        refresh_recovery_points: bool,
        ctx: &LoadableComponentContext<Self>,
    ) {
        self.history_job_id = Some(id.clone());
        self.history_requested_limit = limit.max(1);
        self.history_loading = true;
        if refresh_recovery_points {
            self.recovery_points_loading = true;
            self.recovery_points.clear();
            self.recovery_store.set_data(Vec::new());
            self.recovery_view_store.set_data(Vec::new());
        }

        let history_id = id.clone();
        let history_limit = self.history_requested_limit;
        let link = ctx.link().clone();
        ctx.link().spawn(async move {
            let path = format!(
                "{BASE_URL}/{}/history",
                percent_encode_component(&history_id)
            );
            let runs = http_get(
                &path,
                Some(serde_json::json!({
                    "limit": history_limit,
                })),
            )
            .await;
            link.send_message(Msg::HistoryLoaded(history_id, history_limit, runs));
        });

        if refresh_recovery_points {
            let recovery_id = id;
            let link = ctx.link().clone();
            ctx.link().spawn(async move {
                let path = format!(
                    "{BASE_URL}/{}/recovery-points",
                    percent_encode_component(&recovery_id)
                );
                let points = http_get(&path, None).await;
                link.send_message(Msg::RecoveryPointsLoaded(recovery_id, points));
            });
        }
    }

    fn load_failover_for_job_id(&mut self, id: String, ctx: &LoadableComponentContext<Self>) {
        let rec = {
            let store = self.store.read();
            store.lookup_record(&id.clone().into()).cloned()
        };
        if let Some(rec) = rec {
            let job_changed = self.failover_job_id.as_deref() != Some(id.as_str());
            self.failover_job_id = Some(id.clone());
            if job_changed || !self.failover_form_dirty {
                self.set_failover_defaults_for_job(&rec);
            }
            if job_changed {
                self.failover_running = false;
                self.failover_last_task = None;
                self.failback_running = false;
                self.failback_last_task = None;
                self.failback_precheck = None;
            }
            self.recovery_points_loading = true;
            self.failover_records_loading = true;
            self.recovery_points.clear();
            self.recovery_store.set_data(Vec::new());
            self.recovery_view_store.set_data(Vec::new());
            self.failover_records.clear();
            self.failover_record_store.set_data(Vec::new());

            let recovery_id = id.clone();
            let link = ctx.link().clone();
            ctx.link().spawn(async move {
                let path = format!(
                    "{BASE_URL}/{}/recovery-points",
                    percent_encode_component(&recovery_id)
                );
                let points = http_get(&path, None).await;
                link.send_message(Msg::RecoveryPointsLoaded(recovery_id, points));
            });

            let records_id = id;
            let link = ctx.link().clone();
            ctx.link().spawn(async move {
                let path = format!(
                    "{BASE_URL}/{}/failover-records",
                    percent_encode_component(&records_id)
                );
                let records = http_get(&path, None).await;
                link.send_message(Msg::FailoverRecordsLoaded(records_id, records));
            });
        }
    }

    fn selected_history_run(&self) -> Option<OffsiteReplicationRun> {
        let key = self.history_selection.selected_key()?;
        self.history_store.read().lookup_record(&key).cloned()
    }

    fn selected_recovery_point(&self) -> Option<OffsiteRecoveryPoint> {
        let key = self.recovery_selection.selected_key()?;
        self.recovery_store.read().lookup_record(&key).cloned()
    }

    fn selected_failover_record(&self) -> Option<OffsiteFailoverRecord> {
        let key = self.failover_record_selection.selected_key()?;
        self.failover_record_store
            .read()
            .lookup_record(&key)
            .cloned()
    }

    fn selected_failover_job(&self) -> Option<OffsiteReplicationJobStatus> {
        let id = self.failover_job_id.as_ref()?;
        self.store.read().lookup_record(&id.clone().into()).cloned()
    }

    fn set_failover_defaults_for_job(&mut self, job: &OffsiteReplicationJobStatus) {
        self.failover_vmid_input = job
            .job
            .vmid
            .saturating_add(FAILOVER_VMID_OFFSET)
            .to_string();
        self.failover_name_input = format!("vm{}-dr", job.job.vmid);
        self.failover_snapshot_input.clear();
        self.failover_start_guest = false;
        self.failover_form_dirty = false;
        self.failover_advanced_expanded = false;
        self.failback_restore_mode = OffsiteFailbackRestoreMode::ReplaceOriginal;
        self.failback_restore_vmid_input = job.job.vmid.to_string();
        self.failback_replace_source_guest = false;
        self.failback_allow_full = false;
        self.failback_cleanup_target = false;
    }

    fn selected_recovery_snapshot(&self) -> Result<String, Error> {
        let snapshot = self.failover_snapshot_input.trim();
        if snapshot.is_empty() {
            bail!("No recovery snapshot selected");
        }
        if !self
            .recovery_points
            .iter()
            .any(|point| point.snapshot == snapshot)
        {
            bail!("Selected recovery snapshot is no longer available");
        }

        Ok(snapshot.to_string())
    }

    fn recoverable_snapshot_set(&self) -> HashSet<&str> {
        self.recovery_points
            .iter()
            .map(|point| point.snapshot.as_str())
            .collect()
    }

    fn failover_action_for_selection(&self, start_guest: bool) -> Result<FailoverAction, Error> {
        let job = self
            .selected_failover_job()
            .ok_or_else(|| anyhow::format_err!("No replication job selected"))?;
        let snapshot = self.selected_recovery_snapshot()?;

        let recovery_vmid = self
            .failover_vmid_input
            .trim()
            .parse::<u32>()
            .map_err(|_| anyhow::format_err!("Recovery VMID must be a valid number"))?;
        if recovery_vmid == 0 {
            bail!("Recovery VMID must be greater than zero");
        }

        let recovered_name = {
            let value = self.failover_name_input.trim().to_string();
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        };

        Ok(FailoverAction {
            job_id: job.job.id,
            request: OffsiteFailoverRequest {
                snapshot,
                recovery_vmid,
                recovered_name,
                start_guest,
            },
        })
    }

    fn failback_action_for_selection(&self, start_guest: bool) -> Result<FailbackAction, Error> {
        let job = self
            .selected_failover_job()
            .ok_or_else(|| anyhow::format_err!("No replication job selected"))?;
        let record = self
            .selected_failover_record()
            .ok_or_else(|| anyhow::format_err!("No promoted guest selected for failback"))?;
        if record.lifecycle != OffsiteFailoverLifecycle::Active {
            bail!("Selected promoted guest is not active");
        }
        if record.target_guest_state == OffsiteGuestState::Missing {
            bail!("Selected promoted guest no longer exists on the target node");
        }
        let restore_vmid = self
            .failback_restore_vmid_input
            .trim()
            .parse::<u32>()
            .map_err(|_| anyhow::format_err!("Restore VMID must be a valid number"))?;
        if restore_vmid == 0 {
            bail!("Restore VMID must be greater than zero");
        }

        Ok(FailbackAction {
            job_id: job.job.id.clone(),
            request: OffsiteFailbackRequest {
                recovery_vmid: record.recovery_vmid,
                restore_vmid,
                restore_mode: self.failback_restore_mode,
                recovered_name: None,
                start_guest,
                allow_full: self.failback_allow_full,
                force: self.failback_replace_source_guest,
                cleanup_target: self.failback_cleanup_target,
            },
        })
    }

    fn create_add_dialog(&self, ctx: &LoadableComponentContext<Self>) -> Html {
        let on_prepare = ctx.link().callback(Msg::PrepareSsh);
        let on_generate_key = ctx.link().callback(Msg::GenerateSshKey);
        let on_prepare_details = ctx.link().callback(|_| Msg::OpenPrepareSshDetails);
        let prepare_running = self.ssh_prepare_running;
        let prepare_result = self.ssh_prepare_result.clone();
        let prepare_error = self.ssh_prepare_error.clone();
        let prepare_details_expanded = self.ssh_prepare_details_expanded;
        let prepare_last_details = self.ssh_prepare_last_details.clone();
        let keygen_running = self.ssh_keygen_running;
        let keygen_error = self.ssh_keygen_error.clone();
        let keygen_overwrite_armed = self.ssh_keygen_overwrite_armed;
        EditWindow::new(tr!("Add") + ": " + &tr!("Off-site Replication Job"))
            .renderer(move |form_ctx| {
                input_panel(
                    form_ctx,
                    InputPanelMode::Create,
                    Some(on_prepare.clone()),
                    Some(on_generate_key.clone()),
                    Some(on_prepare_details.clone()),
                    prepare_running,
                    prepare_result.clone(),
                    prepare_error.clone(),
                    prepare_details_expanded,
                    prepare_last_details.clone(),
                    keygen_running,
                    keygen_error.clone(),
                    keygen_overwrite_armed,
                )
            })
            .on_submit(create_job)
            .on_done(ctx.link().callback(|_| Msg::Reload))
            .into()
    }

    fn create_edit_dialog(&self, selection: Key, ctx: &LoadableComponentContext<Self>) -> Html {
        let id = selection.to_string();
        let edit_id = id.clone();
        let submit_id = id.clone();
        let path_id = percent_encode_component(&id);
        let on_prepare = ctx.link().callback(Msg::PrepareSsh);
        let on_generate_key = ctx.link().callback(Msg::GenerateSshKey);
        let on_prepare_details = ctx.link().callback(|_| Msg::OpenPrepareSshDetails);
        let prepare_running = self.ssh_prepare_running;
        let prepare_result = self.ssh_prepare_result.clone();
        let prepare_error = self.ssh_prepare_error.clone();
        let prepare_details_expanded = self.ssh_prepare_details_expanded;
        let prepare_last_details = self.ssh_prepare_last_details.clone();
        let keygen_running = self.ssh_keygen_running;
        let keygen_error = self.ssh_keygen_error.clone();
        let keygen_overwrite_armed = self.ssh_keygen_overwrite_armed;
        EditWindow::new(tr!("Edit") + ": " + &tr!("Off-site Replication Job"))
            .renderer(move |form_ctx| {
                input_panel(
                    form_ctx,
                    InputPanelMode::Edit(edit_id.clone()),
                    Some(on_prepare.clone()),
                    Some(on_generate_key.clone()),
                    Some(on_prepare_details.clone()),
                    prepare_running,
                    prepare_result.clone(),
                    prepare_error.clone(),
                    prepare_details_expanded,
                    prepare_last_details.clone(),
                    keygen_running,
                    keygen_error.clone(),
                    keygen_overwrite_armed,
                )
            })
            .loader(format!("{BASE_URL}/{path_id}"))
            .on_submit(move |form_ctx| update_job(submit_id.clone(), form_ctx))
            .on_done(ctx.link().callback(|_| Msg::Reload))
            .into()
    }

    fn create_job_picker_dialog(
        &self,
        title: String,
        target: JobPickerTarget,
        ctx: &LoadableComponentContext<Self>,
    ) -> Html {
        let filter_clear_icon = if self.job_picker_filter_text.is_empty() {
            ""
        } else {
            "fa fa-times"
        };
        let visible = self.job_picker_store.read().data().len();
        let total = self.store.read().data().len();

        Dialog::new(title)
            .min_width(920)
            .min_height(560)
            .max_height("90vh")
            .resizable(true)
            .on_close(ctx.link().callback(|_| Msg::CloseJobPicker))
            .with_child(
                Column::new()
                    .class(pwt::css::FlexFit)
                    .with_child(
                        Toolbar::new()
                            .border_bottom(true)
                            .with_child(tr!("Filter"))
                            .with_child(
                                Field::new()
                                    .style("width", "420px")
                                    .placeholder(tr!(
                                        "Filter jobs (id, source, target, vmid, schedule)"
                                    ))
                                    .value(self.job_picker_filter_text.clone())
                                    .with_trigger(
                                        Trigger::new(filter_clear_icon)
                                            .on_activate(ctx.link().callback(|_| {
                                                Msg::JobPickerFilterChanged(String::new())
                                            })),
                                        true,
                                    )
                                    .on_input(ctx.link().callback(Msg::JobPickerFilterChanged)),
                            )
                            .with_flex_spacer()
                            .with_child(html! {<span style="opacity:0.75;">{format!("{}: {visible}/{total}", tr!("Visible"))}</span>})
                            .with_child(
                                Button::new(tr!("Select"))
                                    .icon_class("fa fa-check")
                                    .disabled(self.job_picker_selection.selected_key().is_none())
                                    .on_activate({
                                        let link = ctx.link().clone();
                                        move |_| {
                                            link.send_message(match target {
                                                JobPickerTarget::History => {
                                                    Msg::ApplyHistoryJobPicker
                                                }
                                                JobPickerTarget::Failover => {
                                                    Msg::ApplyFailoverJobPicker
                                                }
                                                JobPickerTarget::RunNow => {
                                                    Msg::ApplyRunNowJobPicker
                                                }
                                            })
                                        }
                                    }),
                            ),
                    )
                    .with_child(
                        DataTable::new(self.job_picker_columns.clone(), self.job_picker_store.clone())
                            .class(pwt::css::FlexFit)
                            .selection(self.job_picker_selection.clone())
                            .on_row_dblclick({
                                let link = ctx.link().clone();
                                move |_: &mut _| {
                                    link.send_message(match target {
                                        JobPickerTarget::History => Msg::ApplyHistoryJobPicker,
                                        JobPickerTarget::Failover => Msg::ApplyFailoverJobPicker,
                                        JobPickerTarget::RunNow => Msg::ApplyRunNowJobPicker,
                                    });
                                }
                            }),
                    ),
            )
            .into()
    }
}

impl LoadableComponent for OffsiteReplicationPanelComp {
    type Message = Msg;
    type Properties = OffsiteReplicationPanel;
    type ViewState = ViewState;

    fn create(ctx: &LoadableComponentContext<Self>) -> Self {
        let selection = Selection::new().on_select({
            let link = ctx.link().clone();
            move |_| link.send_redraw()
        });
        let main_tab_selection =
            Selection::new().on_select(ctx.link().callback(|_| Msg::MainTabChanged));
        let history_selection = Selection::new().on_select({
            let link = ctx.link().clone();
            move |_| link.send_redraw()
        });
        let recovery_selection = Selection::new().on_select({
            let link = ctx.link().clone();
            move |_| link.send_redraw()
        });
        let failover_record_selection =
            Selection::new().on_select(ctx.link().callback(|_| Msg::FailbackRecordSelected));
        let job_picker_selection = Selection::new().on_select({
            let link = ctx.link().clone();
            move |_| link.send_redraw()
        });

        let mut state = LoadableComponentState::new();
        state.set_task_base_url("/nodes/localhost/tasks".into());

        let panel = Self {
            state,
            store: Store::with_extract_key(|item: &OffsiteReplicationJobStatus| {
                item.job.id.clone().into()
            }),
            selection,
            main_tab_selection,
            columns: Self::columns(),
            jobs_filter_text: String::new(),
            jobs_view_store: Store::with_extract_key(|item: &OffsiteReplicationJobStatus| {
                item.job.id.clone().into()
            }),
            run_now_submitting: false,
            run_now_last_task: None,
            run_now_feedback: None,
            history_job_id: None,
            history_loading: false,
            history_runs: Vec::new(),
            history_store: Store::with_extract_key(history_run_key),
            history_filter_text: String::new(),
            history_filter_details_text: String::new(),
            history_filter_result: "all".to_string(),
            history_filter_mode: "all".to_string(),
            history_filter_recoverable: "all".to_string(),
            history_filters_expanded: false,
            history_view_store: Store::with_extract_key(history_run_key),
            history_selection,
            history_requested_limit: HISTORY_PAGE_SIZE,
            history_rows_choice: HISTORY_PAGE_SIZE.to_string(),
            failover_job_id: None,
            recovery_points_loading: false,
            recovery_points: Vec::new(),
            recovery_store: Store::with_extract_key(|item: &OffsiteRecoveryPoint| {
                item.snapshot.clone().into()
            }),
            recovery_selection,
            failover_records_loading: false,
            failover_records: Vec::new(),
            failover_record_store: Store::with_extract_key(failover_record_key),
            failover_record_selection,
            failover_record_columns: Self::failover_record_columns(),
            recovery_filter_text: String::new(),
            recovery_filter_mode: "all".to_string(),
            recovery_filter_recoverable: "all".to_string(),
            recovery_filters_expanded: false,
            recovery_view_store: Store::with_extract_key(|item: &OffsiteRecoveryPoint| {
                item.snapshot.clone().into()
            }),
            recovery_columns: Self::recovery_columns(),
            failover_snapshot_input: String::new(),
            failover_vmid_input: String::new(),
            failover_name_input: String::new(),
            failover_start_guest: false,
            failover_form_dirty: false,
            failover_advanced_expanded: false,
            failover_running: false,
            failover_last_task: None,
            failback_running: false,
            failback_last_task: None,
            failback_precheck: None,
            failback_replace_source_guest: false,
            failback_restore_mode: OffsiteFailbackRestoreMode::ReplaceOriginal,
            failback_restore_vmid_input: String::new(),
            failback_allow_full: false,
            failback_cleanup_target: false,
            recovery_delete_running: false,
            recovery_delete_candidate: None,
            job_picker_filter_text: String::new(),
            job_picker_store: Store::with_extract_key(|item: &OffsiteReplicationJobStatus| {
                item.job.id.clone().into()
            }),
            job_picker_selection,
            job_picker_columns: Self::job_picker_columns(),
            remove_purge_target_snapshots: false,
            auto_refresh_enabled: false,
            auto_refresh_timer: None,
            ssh_prepare_running: false,
            ssh_prepare_result: None,
            ssh_prepare_error: None,
            ssh_prepare_details_expanded: false,
            ssh_prepare_last_details: String::new(),
            ssh_keygen_running: false,
            ssh_keygen_error: None,
            ssh_keygen_overwrite_armed: false,
        };
        panel
    }

    fn update(&mut self, ctx: &LoadableComponentContext<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::LoadFinished(data) => {
                self.store.set_data(data);
                self.ensure_table_selection();
                self.normalize_selected_job_ids();
                self.sync_jobs_view_store();
                self.sync_job_picker_store();
                if self.history_job_id.is_none() {
                    self.history_job_id = self.default_job_id();
                }
                if self.failover_job_id.is_none() {
                    self.failover_job_id = self.default_job_id();
                }

                match self.current_tab().as_str() {
                    TAB_METRICS => {
                        if let Some(id) = self
                            .history_job_id
                            .clone()
                            .or_else(|| self.default_job_id())
                        {
                            let request_limit = self.requested_history_limit_for_job(&id);
                            self.load_history_for_job_id(id, request_limit, true, ctx);
                        }
                    }
                    TAB_FAILOVER => {
                        if let Some(id) = self
                            .failover_job_id
                            .clone()
                            .or_else(|| self.default_job_id())
                        {
                            self.load_failover_for_job_id(id, ctx);
                        }
                    }
                    _ => {}
                }
            }
            Msg::ToggleAutoRefresh(value) => {
                self.auto_refresh_enabled = value;
                self.auto_refresh_timer = None;
                if value {
                    self.schedule_auto_refresh(ctx);
                }
            }
            Msg::AutoRefreshTick => {
                self.auto_refresh_timer = None;
                if self.auto_refresh_enabled
                    && !self.loading()
                    && !self.run_now_submitting
                    && !self.failover_running
                    && !self.failback_running
                    && !self.history_loading
                    && !self.recovery_points_loading
                    && !self.failover_records_loading
                {
                    self.refresh_active_tab(ctx);
                }
                self.schedule_auto_refresh(ctx);
            }
            Msg::OpenCreate => {
                self.ssh_prepare_running = false;
                self.ssh_prepare_result = None;
                self.ssh_prepare_error = None;
                self.ssh_prepare_details_expanded = false;
                self.ssh_prepare_last_details.clear();
                self.ssh_keygen_running = false;
                self.ssh_keygen_error = None;
                self.ssh_keygen_overwrite_armed = false;
                ctx.link().change_view(Some(ViewState::Create));
            }
            Msg::OpenEdit => {
                self.ssh_prepare_running = false;
                self.ssh_prepare_result = None;
                self.ssh_prepare_error = None;
                self.ssh_prepare_details_expanded = false;
                self.ssh_prepare_last_details.clear();
                self.ssh_keygen_running = false;
                self.ssh_keygen_error = None;
                self.ssh_keygen_overwrite_armed = false;
                ctx.link().change_view(Some(ViewState::Edit));
            }
            Msg::MainTabChanged => match self.current_tab().as_str() {
                TAB_METRICS => {
                    if let Some(id) = self
                        .history_job_id
                        .clone()
                        .or_else(|| self.default_job_id())
                    {
                        let request_limit = self.requested_history_limit_for_job(&id);
                        self.load_history_for_job_id(id, request_limit, true, ctx);
                    }
                }
                TAB_FAILOVER => {
                    if let Some(id) = self
                        .failover_job_id
                        .clone()
                        .or_else(|| self.default_job_id())
                    {
                        self.load_failover_for_job_id(id, ctx);
                    }
                }
                TAB_JOBS => self.ensure_table_selection(),
                _ => {}
            },
            Msg::Reload => {
                ctx.link().change_view(None);
                ctx.link().send_reload();
            }
            Msg::RefreshActive => self.refresh_active_tab(ctx),
            Msg::RemovePurgeChanged(value) => {
                self.remove_purge_target_snapshots = value;
            }
            Msg::Remove(key, purge_target_snapshots) => {
                if let Some(rec) = self.store.read().lookup_record(&key) {
                    let id = rec.job.id.clone();
                    let link = ctx.link().clone();
                    ctx.link().spawn(async move {
                        let path = format!("{BASE_URL}/{}", percent_encode_component(&id));
                        let body = if purge_target_snapshots {
                            Some(serde_json::json!({
                                "purge-target-snapshots": true,
                            }))
                        } else {
                            None
                        };
                        if let Err(err) = http_delete(path, body).await {
                            link.show_error(
                                tr!("Error"),
                                tr!("Could not remove job '{0}': {1}", id, err),
                                true,
                            );
                        }
                        link.send_message(Msg::Reload);
                    });
                }
                self.remove_purge_target_snapshots = false;
            }
            Msg::RunNowActive => {
                if let Some(id) = self.active_run_job_id() {
                    ctx.link().send_message(Msg::RunNowById(id));
                } else {
                    ctx.link().show_error(
                        tr!("Run Now"),
                        tr!("Select a replication job first."),
                        true,
                    );
                }
            }
            Msg::RunNowById(id) => {
                if self
                    .store
                    .read()
                    .lookup_record(&id.clone().into())
                    .is_some()
                {
                    self.run_now_submitting = true;
                    self.run_now_feedback = None;
                    let link = ctx.link().clone();
                    ctx.link().spawn(async move {
                        let path = format!("{BASE_URL}/{}/run-now", percent_encode_component(&id));
                        let result = http_post(&path, None::<Value>).await;
                        link.send_message(Msg::RunNowFinished(id, result));
                    });
                } else {
                    ctx.link().show_error(
                        tr!("Run Now"),
                        tr!("The selected job is no longer available."),
                        true,
                    );
                }
            }
            Msg::RunNowFinished(id, result) => {
                self.run_now_submitting = false;
                match result {
                    Ok(upid) => {
                        self.run_now_last_task = Some(upid.clone());
                        self.run_now_feedback = Some(RunNowFeedback { job_id: id, upid });
                    }
                    Err(err) => {
                        self.run_now_feedback = None;
                        ctx.link()
                            .show_error(tr!("Run failed"), format!("{id}: {err}"), true);
                    }
                }
                ctx.link().send_message(Msg::Reload);
            }
            Msg::OpenRunNowTaskLog => {
                if let Some(feedback) = self.run_now_feedback.as_ref() {
                    ctx.link().show_task_log(feedback.upid.clone(), None);
                } else {
                    ctx.link()
                        .show_error(tr!("Run Now"), tr!("No run task to open."), true);
                }
            }
            Msg::DismissRunNowFeedback => {
                self.run_now_feedback = None;
            }
            Msg::PrepareSsh(form_data) => {
                if self.ssh_prepare_running || self.ssh_keygen_running {
                    return false;
                }
                let request = match parse_ssh_prepare_request(form_data) {
                    Ok(request) => request,
                    Err(err) => {
                        self.ssh_prepare_result = None;
                        self.ssh_prepare_error = Some(err.to_string());
                        self.ssh_prepare_last_details =
                            format!("{}:\n{}", tr!("Preparation request validation failed"), err);
                        self.ssh_prepare_details_expanded = true;
                        return true;
                    }
                };

                self.ssh_prepare_running = true;
                self.ssh_prepare_result = None;
                self.ssh_prepare_error = None;
                self.ssh_prepare_details_expanded = false;
                self.ssh_prepare_last_details.clear();
                self.ssh_keygen_error = None;
                self.ssh_keygen_overwrite_armed = false;
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let payload = serde_json::to_value(request).ok();
                    let result = http_post(SSH_PREPARE_URL, payload).await;
                    link.send_message(Msg::PrepareSshFinished(result));
                });
            }
            Msg::PrepareSshFinished(result) => {
                self.ssh_prepare_running = false;
                match result {
                    Ok(result) => {
                        let missing_private_key = !result.ok
                            && result.steps.iter().any(|step| {
                                !step.ok
                                    && step.code.as_deref()
                                        == Some(SSH_STEP_CODE_MISSING_PRIVATE_KEY)
                            });
                        self.ssh_prepare_last_details = format_ssh_prepare_details(&result);
                        self.ssh_prepare_details_expanded = !result.ok;
                        self.ssh_prepare_error = None;
                        if !missing_private_key {
                            self.ssh_keygen_overwrite_armed = false;
                        }
                        self.ssh_prepare_result = Some(result);
                    }
                    Err(err) => {
                        self.ssh_prepare_result = None;
                        self.ssh_prepare_error = Some(err.to_string());
                        self.ssh_prepare_last_details =
                            format!("{}:\n{err}", tr!("Preparation request failed"));
                        self.ssh_prepare_details_expanded = true;
                    }
                }
            }
            Msg::GenerateSshKey(form_data) => {
                if self.ssh_keygen_running || self.ssh_prepare_running {
                    return false;
                }

                let prepare_request = match parse_ssh_prepare_request(form_data) {
                    Ok(request) => request,
                    Err(err) => {
                        self.ssh_keygen_error = Some(format!(
                            "{}: {err}",
                            tr!("Key generation request validation failed")
                        ));
                        return true;
                    }
                };
                let keygen_request = OffsiteSshKeygenRequest {
                    source_remote: prepare_request.source_remote.clone(),
                    vmid: prepare_request.vmid,
                    target_remote: prepare_request.target_remote.clone(),
                    ssh_private_key: prepare_request.ssh_private_key.clone(),
                    overwrite: self.ssh_keygen_overwrite_armed,
                };
                self.ssh_keygen_running = true;
                self.ssh_keygen_error = None;
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let payload = serde_json::to_value(keygen_request).ok();
                    let result = http_post(SSH_KEYGEN_URL, payload).await;
                    link.send_message(Msg::GenerateSshKeyFinished(prepare_request, result));
                });
            }
            Msg::GenerateSshKeyFinished(prepare_request, result) => {
                self.ssh_keygen_running = false;
                match result {
                    Ok(result) => {
                        if result.ok {
                            self.ssh_keygen_overwrite_armed = false;
                            self.ssh_keygen_error = Some(format!(
                                "{} {} ({})",
                                tr!("SSH key prepared:"),
                                result.message,
                                result
                                    .fingerprint
                                    .clone()
                                    .unwrap_or_else(|| tr!("fingerprint unavailable").to_string()),
                            ));
                            self.ssh_prepare_running = true;
                            self.ssh_prepare_result = None;
                            self.ssh_prepare_error = None;
                            self.ssh_prepare_details_expanded = false;
                            self.ssh_prepare_last_details.clear();
                            let link = ctx.link().clone();
                            ctx.link().spawn(async move {
                                let payload = serde_json::to_value(prepare_request).ok();
                                let result = http_post(SSH_PREPARE_URL, payload).await;
                                link.send_message(Msg::PrepareSshFinished(result));
                            });
                        } else if result.status == "exists" {
                            self.ssh_keygen_overwrite_armed = true;
                            self.ssh_keygen_error = Some(format!(
                                "{} {}",
                                tr!("SSH key already exists."),
                                tr!("Click 'Overwrite Key' to replace it."),
                            ));
                        } else {
                            self.ssh_keygen_error = Some(result.message);
                        }
                    }
                    Err(err) => {
                        self.ssh_keygen_error = Some(format!(
                            "{}: {err}",
                            tr!("SSH key generation request failed")
                        ));
                    }
                }
            }
            Msg::OpenPrepareSshDetails => {
                if !self.ssh_prepare_last_details.trim().is_empty() {
                    self.ssh_prepare_details_expanded = !self.ssh_prepare_details_expanded;
                }
            }
            Msg::OpenHistory(key) => {
                let rec = {
                    let store = self.store.read();
                    store.lookup_record(&key).cloned()
                };
                if let Some(rec) = rec {
                    let id = rec.job.id.clone();
                    self.history_job_id = Some(id.clone());
                    if self.current_tab() == TAB_METRICS {
                        let request_limit = self.requested_history_limit_for_job(&id);
                        self.load_history_for_job_id(id, request_limit, true, ctx);
                    } else {
                        self.main_tab_selection.select(TAB_METRICS);
                    }
                }
            }
            Msg::OpenFailover(key) => {
                let rec = {
                    let store = self.store.read();
                    store.lookup_record(&key).cloned()
                };
                if let Some(rec) = rec {
                    let id = rec.job.id.clone();
                    self.failover_job_id = Some(id.clone());
                    if self.current_tab() == TAB_FAILOVER {
                        self.load_failover_for_job_id(id, ctx);
                    } else {
                        self.main_tab_selection.select(TAB_FAILOVER);
                    }
                }
            }
            Msg::HistoryLoaded(id, requested_limit, result) => {
                if self.history_job_id.as_deref() != Some(id.as_str()) {
                    return false;
                }
                if requested_limit != self.history_requested_limit {
                    return false;
                }
                self.history_loading = false;
                match result {
                    Ok(mut runs) => {
                        runs.sort_by(|a, b| b.start_time.cmp(&a.start_time));
                        let selected_key = runs.first().map(history_run_key);
                        self.history_runs = runs.clone();
                        self.history_store.set_data(runs);
                        self.sync_history_view_store();
                        if let Some(key) = selected_key {
                            self.history_selection.select(key);
                        }
                    }
                    Err(err) => ctx.link().show_error(tr!("History"), err.to_string(), true),
                }
            }
            Msg::RecoveryPointsLoaded(id, result) => {
                let id_matches = self.history_job_id.as_deref() == Some(id.as_str())
                    || self.failover_job_id.as_deref() == Some(id.as_str());
                if !id_matches {
                    return false;
                }
                self.recovery_points_loading = false;
                match result {
                    Ok(mut points) => {
                        points.sort_by(|a, b| b.end_time.cmp(&a.end_time));
                        let selected_still_exists = points
                            .iter()
                            .any(|point| point.snapshot == self.failover_snapshot_input);
                        if !selected_still_exists {
                            self.failover_snapshot_input = points
                                .first()
                                .map(|point| point.snapshot.clone())
                                .unwrap_or_default();
                            self.failback_precheck = None;
                        }
                        if let Some(current_key) = self.recovery_selection.selected_key() {
                            let current = current_key.to_string();
                            if !points.iter().any(|point| point.snapshot == current) {
                                if let Some(first) = points.first() {
                                    self.recovery_selection.select(first.snapshot.clone());
                                }
                            }
                        } else if let Some(first) = points.first() {
                            self.recovery_selection.select(first.snapshot.clone());
                        }
                        self.recovery_store.set_data(points.clone());
                        self.recovery_points = points;
                        self.sync_recovery_view_store();
                    }
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Recovery points"), err.to_string(), true)
                    }
                }
            }
            Msg::FailoverRecordsLoaded(id, result) => {
                if self.failover_job_id.as_deref() != Some(id.as_str()) {
                    return false;
                }
                self.failover_records_loading = false;
                match result {
                    Ok(mut records) => {
                        records.sort_by(|a, b| b.failover_time.cmp(&a.failover_time));
                        let preferred = records
                            .iter()
                            .find(|record| {
                                record.lifecycle == OffsiteFailoverLifecycle::Active
                                    && record.target_guest_state != OffsiteGuestState::Missing
                            })
                            .or_else(|| {
                                records.iter().find(|record| {
                                    record.lifecycle == OffsiteFailoverLifecycle::Active
                                })
                            })
                            .or_else(|| records.first())
                            .map(failover_record_key);
                        if let Some(current_key) = self.failover_record_selection.selected_key() {
                            if !records
                                .iter()
                                .any(|record| failover_record_key(record) == current_key)
                            {
                                if let Some(preferred) = preferred.clone() {
                                    self.failover_record_selection.select(preferred);
                                }
                            }
                        } else if let Some(preferred) = preferred {
                            self.failover_record_selection.select(preferred);
                        }
                        self.failover_record_store.set_data(records.clone());
                        self.failover_records = records;
                        self.failback_precheck = None;
                        self.failback_replace_source_guest = false;
                    }
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Failover records"), err.to_string(), true)
                    }
                }
            }
            Msg::RequestFailover(start_guest) => {
                match self.failover_action_for_selection(start_guest) {
                    Ok(action) => {
                        ctx.link()
                            .send_message(Msg::TriggerFailover(action.job_id, action.request));
                    }
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Failover"), err.to_string(), true);
                    }
                }
            }
            Msg::TriggerFailover(id, request) => {
                let payload = match serde_json::to_value(request) {
                    Ok(value) => value,
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Failover"), err.to_string(), true);
                        return false;
                    }
                };

                self.failover_running = true;
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let path = format!("{BASE_URL}/{}/failover", percent_encode_component(&id));
                    let result = http_post(&path, Some(payload)).await;
                    link.send_message(Msg::FailoverFinished(result));
                });
            }
            Msg::FailoverFinished(result) => {
                self.failover_running = false;
                match result {
                    Ok(upid) => {
                        self.failover_last_task = Some(upid.clone());
                        ctx.link().show_task_progress(upid);
                    }
                    Err(err) => ctx
                        .link()
                        .show_error(tr!("Failover"), err.to_string(), true),
                }
                if let Some(id) = self.failover_job_id.clone() {
                    ctx.link().send_message(Msg::OpenFailover(id.into()));
                }
                ctx.link().send_reload();
            }
            Msg::FailbackPrecheck => match self.failback_action_for_selection(false) {
                Ok(action) => {
                    let payload = match serde_json::to_value(action.request) {
                        Ok(value) => value,
                        Err(err) => {
                            ctx.link()
                                .show_error(tr!("Failback"), err.to_string(), true);
                            return false;
                        }
                    };
                    let link = ctx.link().clone();
                    ctx.link().spawn(async move {
                        let path = format!(
                            "{BASE_URL}/{}/failback-precheck",
                            percent_encode_component(&action.job_id)
                        );
                        let result = http_post(&path, Some(payload)).await;
                        link.send_message(Msg::FailbackPrecheckFinished(result));
                    });
                }
                Err(err) => ctx
                    .link()
                    .show_error(tr!("Failback"), err.to_string(), true),
            },
            Msg::FailbackPrecheckFinished(result) => match result {
                Ok(precheck) => {
                    if !precheck.source_guest_exists {
                        self.failback_replace_source_guest = false;
                    }
                    if self.failback_restore_mode == OffsiteFailbackRestoreMode::RestoreAsNew
                        && self.failback_restore_vmid_input.trim().is_empty()
                    {
                        self.failback_restore_vmid_input =
                            precheck.suggested_restore_vmid.to_string();
                    }
                    self.failback_precheck = Some(precheck);
                }
                Err(err) => ctx
                    .link()
                    .show_error(tr!("Failback precheck"), err.to_string(), true),
            },
            Msg::RequestFailback(start_guest) => {
                match self.failback_action_for_selection(start_guest) {
                    Ok(action) => {
                        if action.request.force
                            || action.request.allow_full
                            || action.request.cleanup_target
                        {
                            ctx.link()
                                .change_view(Some(ViewState::ConfirmFailback(start_guest)));
                        } else {
                            ctx.link()
                                .send_message(Msg::TriggerFailback(action.job_id, action.request));
                        }
                    }
                    Err(err) => ctx
                        .link()
                        .show_error(tr!("Failback"), err.to_string(), true),
                }
            }
            Msg::TriggerFailback(id, request) => {
                let payload = match serde_json::to_value(request) {
                    Ok(value) => value,
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Failback"), err.to_string(), true);
                        return false;
                    }
                };

                self.failback_running = true;
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let path = format!("{BASE_URL}/{}/failback", percent_encode_component(&id));
                    let result = http_post(&path, Some(payload)).await;
                    link.send_message(Msg::FailbackFinished(result));
                });
            }
            Msg::FailbackFinished(result) => {
                self.failback_running = false;
                match result {
                    Ok(upid) => {
                        self.failback_last_task = Some(upid.clone());
                        ctx.link().show_task_progress(upid);
                    }
                    Err(err) => ctx
                        .link()
                        .show_error(tr!("Failback"), err.to_string(), true),
                }
                if let Some(id) = self.failover_job_id.clone() {
                    ctx.link().send_message(Msg::OpenFailover(id.into()));
                }
                ctx.link().send_reload();
            }
            Msg::RequestAbandonFailover => {
                if self.selected_failover_record().is_some() {
                    ctx.link()
                        .change_view(Some(ViewState::ConfirmAbandonFailover));
                }
            }
            Msg::AbandonFailover(job_id, record_id) => {
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let path = format!(
                        "{BASE_URL}/{}/failover-record-abandon",
                        percent_encode_component(&job_id)
                    );
                    let result =
                        http_post(&path, Some(serde_json::json!({ "record-id": record_id }))).await;
                    link.send_message(Msg::AbandonFailoverFinished(result));
                });
            }
            Msg::AbandonFailoverFinished(result) => {
                ctx.link().change_view(None);
                match result {
                    Ok(()) => {
                        if let Some(id) = self.failover_job_id.clone() {
                            ctx.link().send_message(Msg::OpenFailover(id.into()));
                        }
                        ctx.link().send_reload();
                    }
                    Err(err) => {
                        ctx.link()
                            .show_error(tr!("Archive promoted guest"), err.to_string(), true)
                    }
                }
            }
            Msg::ResumeReplication => {
                let Some(id) = self.failover_job_id.clone() else {
                    return false;
                };
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let path = format!("{BASE_URL}/{}/resume", percent_encode_component(&id));
                    let result = http_post(&path, None).await;
                    link.send_message(Msg::ResumeReplicationFinished(result));
                });
            }
            Msg::ResumeReplicationFinished(result) => match result {
                Ok(()) => {
                    ctx.link().send_reload();
                    if let Some(id) = self.failover_job_id.clone() {
                        ctx.link().send_message(Msg::OpenFailover(id.into()));
                    }
                }
                Err(err) => ctx
                    .link()
                    .show_error(tr!("Resume replication"), err.to_string(), true),
            },
            Msg::OpenDeleteRecoverySnapshot => {
                let Some(point) = self.selected_recovery_point() else {
                    ctx.link().show_error(
                        tr!("Delete snapshot"),
                        tr!("Select a recovery snapshot first."),
                        true,
                    );
                    return true;
                };
                self.recovery_delete_candidate = Some(point.snapshot);
                ctx.link()
                    .change_view(Some(ViewState::DeleteRecoverySnapshot));
            }
            Msg::DeleteRecoverySnapshot(snapshot) => {
                let Some(job_id) = self
                    .failover_job_id
                    .clone()
                    .or_else(|| self.history_job_id.clone())
                else {
                    ctx.link().show_error(
                        tr!("Delete snapshot"),
                        tr!("No off-site replication job is selected."),
                        true,
                    );
                    return true;
                };
                self.recovery_delete_running = true;
                self.recovery_delete_candidate = Some(snapshot.clone());
                let link = ctx.link().clone();
                ctx.link().spawn(async move {
                    let path = format!(
                        "{BASE_URL}/{}/recovery-snapshot",
                        percent_encode_component(&job_id)
                    );
                    let result =
                        http_delete(path, Some(serde_json::json!({ "snapshot": snapshot }))).await;
                    link.send_message(Msg::DeleteRecoverySnapshotFinished(
                        job_id, snapshot, result,
                    ));
                });
            }
            Msg::DeleteRecoverySnapshotFinished(job_id, snapshot, result) => {
                self.recovery_delete_running = false;
                self.recovery_delete_candidate = None;
                ctx.link().change_view(None);
                match result {
                    Ok(()) => {
                        if self.current_tab() == TAB_METRICS {
                            let limit = self.requested_history_limit_for_job(&job_id);
                            self.load_history_for_job_id(job_id.clone(), limit, true, ctx);
                        } else {
                            self.load_failover_for_job_id(job_id.clone(), ctx);
                        }
                    }
                    Err(err) => ctx.link().show_error(
                        tr!("Delete snapshot"),
                        format!("{snapshot}: {err}"),
                        true,
                    ),
                }
            }
            Msg::UpdateFailoverVmid(value) => {
                self.failover_vmid_input = value;
                self.failover_form_dirty = true;
                self.failback_precheck = None;
            }
            Msg::UpdateFailoverSnapshot(value) => {
                self.failover_snapshot_input = value;
                self.failback_precheck = None;
                self.failback_replace_source_guest = false;
            }
            Msg::UpdateFailoverName(value) => {
                self.failover_name_input = value;
                self.failover_form_dirty = true;
                self.failback_precheck = None;
            }
            Msg::UpdateFailoverStart(value) => {
                self.failover_start_guest = value;
                self.failover_form_dirty = true;
            }
            Msg::UpdateFailbackReplaceSource(value) => {
                self.failback_replace_source_guest = value;
            }
            Msg::UpdateFailbackRestoreMode(value) => {
                self.failback_restore_mode = if value == "restore-as-new" {
                    OffsiteFailbackRestoreMode::RestoreAsNew
                } else {
                    OffsiteFailbackRestoreMode::ReplaceOriginal
                };
                if let Some(job) = self.selected_failover_job() {
                    self.failback_restore_vmid_input = match self.failback_restore_mode {
                        OffsiteFailbackRestoreMode::ReplaceOriginal => job.job.vmid.to_string(),
                        OffsiteFailbackRestoreMode::RestoreAsNew => {
                            job.job.vmid.saturating_add(1_000).to_string()
                        }
                    };
                }
                self.failback_replace_source_guest = false;
                self.failback_allow_full = false;
                self.failback_precheck = None;
            }
            Msg::UpdateFailbackRestoreVmid(value) => {
                self.failback_restore_vmid_input = value;
                self.failback_replace_source_guest = false;
                self.failback_precheck = None;
            }
            Msg::UpdateFailbackAllowFull(value) => {
                self.failback_allow_full = value;
            }
            Msg::UpdateFailbackCleanupTarget(value) => {
                self.failback_cleanup_target = value;
            }
            Msg::FailbackRecordSelected => {
                self.failback_precheck = None;
                self.failback_replace_source_guest = false;
            }
            Msg::ToggleFailoverAdvanced => {
                self.failover_advanced_expanded = !self.failover_advanced_expanded;
            }
            Msg::ResetFailoverDefaults => {
                if let Some(job) = self.selected_failover_job() {
                    self.set_failover_defaults_for_job(&job);
                    self.failback_precheck = None;
                }
            }
            Msg::JobsFilterChanged(value) => {
                self.jobs_filter_text = value;
                self.sync_jobs_view_store();
            }
            Msg::JobsJobChanged(value) => {
                let id = value.trim();
                if !id.is_empty() {
                    self.selection.select(id.to_string());
                }
            }
            Msg::HistoryFilterChanged(value) => {
                self.history_filter_text = value;
                self.sync_history_view_store();
            }
            Msg::HistoryFilterDetailsChanged(value) => {
                self.history_filter_details_text = value;
                self.sync_history_view_store();
            }
            Msg::HistoryFilterResultChanged(value) => {
                self.history_filter_result = value.trim().to_ascii_lowercase();
                self.sync_history_view_store();
            }
            Msg::HistoryFilterModeChanged(value) => {
                self.history_filter_mode = value.trim().to_ascii_lowercase();
                self.sync_history_view_store();
            }
            Msg::HistoryFilterRecoverableChanged(value) => {
                self.history_filter_recoverable = value.trim().to_ascii_lowercase();
                self.sync_history_view_store();
            }
            Msg::ToggleHistoryFilters => {
                self.history_filters_expanded = !self.history_filters_expanded;
            }
            Msg::ClearHistoryFilters => {
                self.history_filter_text.clear();
                self.history_filter_details_text.clear();
                self.history_filter_result = "all".to_string();
                self.history_filter_mode = "all".to_string();
                self.history_filter_recoverable = "all".to_string();
                self.sync_history_view_store();
            }
            Msg::HistoryRowsChanged(value) => {
                if self.current_tab() == TAB_METRICS {
                    if let Some(id) = self
                        .history_job_id
                        .clone()
                        .or_else(|| self.default_job_id())
                    {
                        let configured_limit = self.configured_history_limit_for_job(&id);
                        self.history_rows_choice =
                            self.normalized_history_rows_choice_for_limit(configured_limit, &value);
                        let request_limit = self.requested_history_limit_for_job(&id);
                        self.load_history_for_job_id(id, request_limit, false, ctx);
                    }
                }
            }
            Msg::RecoveryFilterChanged(value) => {
                self.recovery_filter_text = value;
                self.sync_recovery_view_store();
            }
            Msg::RecoveryFilterModeChanged(value) => {
                self.recovery_filter_mode = value.trim().to_ascii_lowercase();
                self.sync_recovery_view_store();
            }
            Msg::RecoveryFilterRecoverableChanged(value) => {
                self.recovery_filter_recoverable = value.trim().to_ascii_lowercase();
                self.sync_recovery_view_store();
            }
            Msg::ToggleRecoveryFilters => {
                self.recovery_filters_expanded = !self.recovery_filters_expanded;
            }
            Msg::ClearRecoveryFilters => {
                self.recovery_filter_text.clear();
                self.recovery_filter_mode = "all".to_string();
                self.recovery_filter_recoverable = "all".to_string();
                self.sync_recovery_view_store();
            }
            Msg::SelectHistoryJob(value) => {
                let id = value.trim().to_string();
                self.history_job_id = (!id.is_empty()).then_some(id.clone());
                if self.current_tab() == TAB_METRICS {
                    if let Some(id) = self
                        .history_job_id
                        .clone()
                        .or_else(|| self.default_job_id())
                    {
                        let request_limit = self.requested_history_limit_for_job(&id);
                        self.load_history_for_job_id(id, request_limit, true, ctx);
                    }
                }
            }
            Msg::SelectFailoverJob(value) => {
                let id = value.trim().to_string();
                self.failover_job_id = (!id.is_empty()).then_some(id.clone());
                if self.current_tab() == TAB_FAILOVER {
                    if let Some(id) = self
                        .failover_job_id
                        .clone()
                        .or_else(|| self.default_job_id())
                    {
                        self.load_failover_for_job_id(id, ctx);
                    }
                }
            }
            Msg::OpenHistoryJobPicker => {
                self.job_picker_filter_text.clear();
                self.sync_job_picker_store();
                if let Some(id) = self
                    .history_job_id
                    .clone()
                    .or_else(|| self.default_job_id())
                {
                    self.job_picker_selection.select(id);
                }
                ctx.link().change_view(Some(ViewState::PickHistoryJob));
            }
            Msg::OpenFailoverJobPicker => {
                self.job_picker_filter_text.clear();
                self.sync_job_picker_store();
                if let Some(id) = self
                    .failover_job_id
                    .clone()
                    .or_else(|| self.default_job_id())
                {
                    self.job_picker_selection.select(id);
                }
                ctx.link().change_view(Some(ViewState::PickFailoverJob));
            }
            Msg::OpenRunNowJobPicker => {
                self.job_picker_filter_text.clear();
                self.sync_job_picker_store();
                if let Some(id) = self.active_run_job_id().or_else(|| self.default_job_id()) {
                    self.job_picker_selection.select(id);
                }
                ctx.link().change_view(Some(ViewState::PickRunNowJob));
            }
            Msg::CloseJobPicker => {
                ctx.link().change_view(None);
            }
            Msg::JobPickerFilterChanged(value) => {
                self.job_picker_filter_text = value;
                self.sync_job_picker_store();
            }
            Msg::ApplyHistoryJobPicker => {
                if let Some(id) = self.job_picker_selection.selected_key() {
                    ctx.link()
                        .send_message(Msg::SelectHistoryJob(id.to_string()));
                }
                ctx.link().change_view(None);
            }
            Msg::ApplyFailoverJobPicker => {
                if let Some(id) = self.job_picker_selection.selected_key() {
                    ctx.link()
                        .send_message(Msg::SelectFailoverJob(id.to_string()));
                }
                ctx.link().change_view(None);
            }
            Msg::ApplyRunNowJobPicker => {
                if let Some(id) = self.job_picker_selection.selected_key() {
                    ctx.link().send_message(Msg::RunNowById(id.to_string()));
                } else {
                    ctx.link()
                        .show_error(tr!("Run Now"), tr!("Select a job first."), true);
                }
                ctx.link().change_view(None);
            }
        }

        true
    }

    fn toolbar(&self, ctx: &LoadableComponentContext<Self>) -> Option<Html> {
        let selected = self.selection.selected_key();
        let selected_key = selected.clone();
        let selected_key_for_history = selected;
        let selected_key_for_failover = selected_key_for_history.clone();
        let run_target = self.active_run_job_id();
        Some(
            Toolbar::new()
                .with_child(
                    Button::new(tr!("Add"))
                        .icon_class("fa fa-plus-circle")
                        .on_activate(ctx.link().callback(|_| Msg::OpenCreate)),
                )
                .with_child(
                    Button::new(tr!("Edit"))
                        .icon_class("fa fa-pencil")
                        .disabled(selected_key.is_none())
                        .on_activate(ctx.link().callback(|_| Msg::OpenEdit)),
                )
                .with_child(
                    Button::new(tr!("Remove"))
                        .icon_class("fa fa-trash-o")
                        .disabled(self.selected_job().is_none())
                        .on_activate(ctx.link().change_view_callback(|_| Some(ViewState::Remove))),
                )
                .with_child(
                    Button::new(if self.run_now_submitting {
                        tr!("Run Now (Starting...)")
                    } else {
                        tr!("Run Now")
                    })
                    .icon_class("fa fa-play")
                    .disabled(run_target.is_none() || self.run_now_submitting)
                    .on_activate(ctx.link().callback(|_| Msg::RunNowActive)),
                )
                .with_child(
                    Button::new(tr!("Run specific job..."))
                        .icon_class("fa fa-search")
                        .disabled(self.store.read().data().is_empty() || self.run_now_submitting)
                        .on_activate(ctx.link().callback(|_| Msg::OpenRunNowJobPicker)),
                )
                .with_child(
                    Button::new(tr!("Metrics"))
                        .icon_class("fa fa-area-chart")
                        .disabled(selected_key_for_history.is_none())
                        .on_activate({
                            let link = ctx.link().clone();
                            move |_| {
                                if let Some(key) = &selected_key_for_history {
                                    link.send_message(Msg::OpenHistory(key.clone()));
                                }
                            }
                        }),
                )
                .with_child(
                    Button::new(tr!("Failover / Restore"))
                        .icon_class("fa fa-bolt")
                        .disabled(selected_key_for_failover.is_none())
                        .on_activate({
                            let link = ctx.link().clone();
                            move |_| {
                                if let Some(key) = &selected_key_for_failover {
                                    link.send_message(Msg::OpenFailover(key.clone()));
                                }
                            }
                        }),
                )
                .with_spacer()
                .with_flex_spacer()
                .with_child(
                    Checkbox::new()
                        .box_label(tr!("Auto Refresh"))
                        .checked(self.auto_refresh_enabled)
                        .on_change(ctx.link().callback(Msg::ToggleAutoRefresh)),
                )
                .with_child({
                    let link = ctx.link().clone();
                    let loading = self.loading();
                    Button::refresh(loading).on_activate(move |_| {
                        link.send_message(Msg::RefreshActive);
                    })
                })
                .into(),
        )
    }

    fn load(
        &self,
        ctx: &LoadableComponentContext<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>> {
        let link = ctx.link().clone();
        Box::pin(async move {
            let data: Vec<OffsiteReplicationJobStatus> = http_get(BASE_URL, None).await?;
            link.send_message(Msg::LoadFinished(data));
            Ok(())
        })
    }

    fn main_view(&self, ctx: &LoadableComponentContext<Self>) -> Html {
        let jobs: Vec<OffsiteReplicationJobStatus> =
            self.store.read().data().iter().cloned().collect();
        let mut job_ids: Vec<String> = jobs.iter().map(|job| job.job.id.clone()).collect();
        job_ids.sort();
        let job_items: Rc<Vec<yew::AttrValue>> =
            Rc::new(job_ids.iter().map(|id| id.clone().into()).collect());
        let has_jobs = !job_items.is_empty();
        let metrics_job_value = self
            .history_job_id
            .clone()
            .or_else(|| self.default_job_id())
            .unwrap_or_default();
        let failover_job_value = self
            .failover_job_id
            .clone()
            .or_else(|| self.default_job_id())
            .unwrap_or_default();
        let selected_job_value = self.default_job_id().unwrap_or_default();

        let jobs_filter_clear_icon = if self.jobs_filter_text.is_empty() {
            ""
        } else {
            "fa fa-times"
        };
        let history_filter_clear_icon = if self.history_filter_text.is_empty() {
            ""
        } else {
            "fa fa-times"
        };
        let recovery_filter_clear_icon = if self.recovery_filter_text.is_empty() {
            ""
        } else {
            "fa fa-times"
        };
        let jobs_visible = self.jobs_view_store.read().data().len();
        let history_visible = self.history_view_store.read().data().len();
        let recovery_visible = self.recovery_view_store.read().data().len();
        let history_filters_active = self.history_filters_active_count();
        let recovery_filters_active = self.recovery_filters_active_count();
        let history_result_items: Rc<Vec<yew::AttrValue>> =
            Rc::new(vec!["all".into(), "ok".into(), "error".into()]);
        let history_mode_items: Rc<Vec<yew::AttrValue>> = Rc::new(vec![
            "all".into(),
            "full".into(),
            "incremental".into(),
            "unknown".into(),
        ]);
        let yes_no_items: Rc<Vec<yew::AttrValue>> =
            Rc::new(vec!["all".into(), "yes".into(), "no".into()]);

        let jobs_tab: Html = Container::new()
            .class(pwt::css::FlexFit)
            .class(pwt::css::ColorScheme::Neutral)
            .class("pwt-content-spacer-padding")
            .with_child(
                Column::new()
                    .class(pwt::css::FlexFit)
                    .gap(3)
                    .with_child(
                        Panel::new()
                            .border(false)
                            .title(tr!("Overview"))
                            .with_child(render_jobs_overview(&jobs))
                            .with_optional_child(self.run_now_last_task.as_ref().map(|task| {
                                html! {
                                    <div style="padding: 0 12px 10px; opacity: 0.82;">
                                        {format!("{}: {task}", tr!("Last Run Now task"))}
                                    </div>
                                }
                            })),
                    )
                    .with_child(
                        Panel::new()
                            .border(true)
                            .title(tr!("Jobs"))
                            .style("height", "460px")
                            .class(pwt::css::FlexFit)
                            .with_child(
                                Toolbar::new()
                                    .border_bottom(true)
                                    .with_child(tr!("Job"))
                                    .with_child(
                                        Combobox::new()
                                            .key("jobs-job-selector")
                                            .style("width", "220px")
                                            .placeholder(tr!("Jump to job"))
                                            .editable(true)
                                            .value(selected_job_value.clone())
                                            .items(job_items.clone())
                                            .disabled(!has_jobs)
                                            .on_change(ctx.link().callback(Msg::JobsJobChanged)),
                                    )
                                    .with_spacer()
                                    .with_child(tr!("Filter"))
                                    .with_child(
                                        Field::new()
                                            .style("width", "280px")
                                            .placeholder(tr!(
                                                "Filter jobs (id, source, target, vmid, schedule)"
                                            ))
                                            .value(self.jobs_filter_text.clone())
                                            .with_trigger(
                                                Trigger::new(jobs_filter_clear_icon).on_activate(
                                                    ctx.link().callback(|_| {
                                                        Msg::JobsFilterChanged(String::new())
                                                    }),
                                                ),
                                                true,
                                            )
                                            .on_input(
                                                ctx.link().callback(Msg::JobsFilterChanged),
                                            ),
                                    )
                                    .with_flex_spacer()
                                    .with_child(
                                        html! {<span style="opacity:0.75;">{format!("{}: {jobs_visible}/{}", tr!("Visible"), jobs.len())}</span>},
                                    ),
                            )
                            .with_child(
                                DataTable::new(self.columns.clone(), self.jobs_view_store.clone())
                                    .class(pwt::css::FlexFit)
                                    .selection(self.selection.clone())
                                    .on_row_dblclick({
                                        let link = ctx.link().clone();
                                        move |_: &mut _| {
                                            link.change_view(Some(ViewState::Edit));
                                        }
                                    }),
                            ),
                    ),
            )
            .into();

        let metrics_tab: Html = if let Some(id) = self
            .history_job_id
            .clone()
            .or_else(|| self.default_job_id())
        {
            let active_history_job = self.store.read().lookup_record(&id.clone().into()).cloned();
            let active_history_guest = active_history_job
                .as_ref()
                .map(|job| format!("{}:{}", guest_type_text(job.job.guest_type), job.job.vmid))
                .unwrap_or_else(|| "-".to_string());
            let active_history_path = active_history_job
                .as_ref()
                .map(|job| format!("{} -> {}", job.job.source_remote, job.job.target_remote))
                .unwrap_or_else(|| "-".to_string());
            let recoverable_snapshots = self.recoverable_snapshot_set();
            let history_columns = history_columns_with_recoverable(
                self.recovery_points
                    .iter()
                    .map(|point| point.snapshot.clone())
                    .collect(),
            );
            let selected_output = self.selected_history_run().map(|run| {
                Panel::new()
                    .title(tr!("Selected Run Log"))
                    .style("height", "190px")
                    .with_child(html! {
                        <pre style="padding: 12px; overflow: auto; white-space: pre-wrap; margin: 0; height: 100%;">{run.output}</pre>
                    })
            });
            let configured_history_limit = self.configured_history_limit_for_job(&id);
            let metrics_rows_value = self.normalized_history_rows_choice_for_limit(
                configured_history_limit,
                &self.history_rows_choice,
            );
            let loaded_runs = self.history_runs.len();
            let history_retention_note = format!(
                "{} {loaded_runs}/{configured_history_limit} {}.",
                tr!("Loaded runs:"),
                tr!("(newest first, retained per-job)")
            );
            let history_rows_items: Rc<Vec<yew::AttrValue>> =
                Rc::new(self.history_rows_items_for_limit(configured_history_limit));
            let run_history_panel_height = "460px";

            Container::new()
                .class(pwt::css::FlexFit)
                .class(pwt::css::ColorScheme::Neutral)
                .class("pwt-content-spacer-padding")
                .with_child(
                    Column::new()
                        .class(pwt::css::FlexFit)
                        .gap(3)
                        .with_child(
                            Panel::new()
                                .border(false)
                                .title(tr!("Summary"))
                                .with_child(
                                    Toolbar::new()
                                        .with_child(
                                            Button::new(tr!("Select Job"))
                                                .icon_class("fa fa-search")
                                                .disabled(!has_jobs || self.history_loading)
                                                .on_activate(
                                                    ctx.link().callback(|_| Msg::OpenHistoryJobPicker),
                                                ),
                                        )
                                        .with_child(html! {
                                            <span style="opacity:0.85;">
                                                {format!("{}: {}", tr!("Job"), metrics_job_value)}
                                            </span>
                                        })
                                        .with_child(html! {
                                            <span style="opacity:0.85;">
                                                {format!("{}: {active_history_guest}", tr!("Guest"))}
                                            </span>
                                        })
                                        .with_child(html! {
                                            <span style="opacity:0.85;">
                                                {format!("{}: {active_history_path}", tr!("Path"))}
                                            </span>
                                        })
                                        .with_flex_spacer()
                                        .with_child(
                                            Button::refresh(self.history_loading || self.recovery_points_loading).on_activate({
                                                let id = id.clone();
                                                let link = ctx.link().clone();
                                                move |_| link.send_message(Msg::OpenHistory(id.clone().into()))
                                            }),
                                        ),
                                )
                                .with_child(render_history_summary_cards(
                                    &self.history_runs,
                                    self.recovery_points.len(),
                                    self.selected_history_run().and_then(|run| run.snapshot),
                                    &recoverable_snapshots,
                                ))
                                .with_child(html! {
                                    <div style="padding: 0 12px 10px; opacity: 0.82;">
                                        {history_retention_note}
                                    </div>
                                }),
                        )
                        .with_child(
                            Panel::new()
                                .border(true)
                                .title(tr!("Run Summary by Mode"))
                                .with_child(render_run_mode_summary_panel(&self.history_runs)),
                        )
                        .with_child(
                            Panel::new()
                                .border(true)
                                .title(tr!("Trend Graphs"))
                                .with_child(render_history_trend_graphs(&self.history_runs)),
                        )
                        .with_child(
                            Panel::new()
                                .border(true)
                                .title(tr!("Run History"))
                                .style("height", run_history_panel_height)
                                .class(pwt::css::FlexFit)
                                .with_child(
                                    Toolbar::new()
                                        .border_bottom(true)
                                        .with_child(
                                            Button::new(format!(
                                                "{} ({history_filters_active})",
                                                tr!("Clear Filter")
                                            ))
                                            .disabled(history_filters_active == 0)
                                            .on_activate(ctx.link().callback(|_| Msg::ClearHistoryFilters)),
                                        )
                                        .with_child(
                                            Button::new(tr!("Filter"))
                                                .icon_class("fa fa-filter")
                                                .on_activate(
                                                    ctx.link().callback(|_| Msg::ToggleHistoryFilters),
                                                ),
                                        )
                                        .with_flex_spacer()
                                        .with_child(tr!("Rows"))
                                        .with_child(
                                            Combobox::new()
                                                .key("metrics-history-rows")
                                                .style("width", "120px")
                                                .editable(false)
                                                .value(metrics_rows_value.clone())
                                                .items(history_rows_items.clone())
                                                .disabled(self.history_loading)
                                                .on_change(
                                                    ctx.link()
                                                        .callback(Msg::HistoryRowsChanged),
                                                ),
                                        )
                                        .with_child(
                                            html! {<span style="opacity:0.75;">{format!("{}: {history_visible}/{configured_history_limit} {}", tr!("Visible"), tr!("retained"))}</span>},
                                        ),
                                )
                                .with_optional_child(if self.history_filters_expanded {
                                    Some(
                                        Panel::new()
                                            .border_bottom(true)
                                            .with_child(
                                                html! {
                                                    <div style="display:grid; gap:12px; padding: 12px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));">
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Result")}</div>
                                                            {Combobox::new()
                                                                .style("width", "100%")
                                                                .editable(false)
                                                                .value(self.history_filter_result.clone())
                                                                .items(history_result_items.clone())
                                                                .on_change(ctx.link().callback(Msg::HistoryFilterResultChanged))}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Mode")}</div>
                                                            {Combobox::new()
                                                                .style("width", "100%")
                                                                .editable(false)
                                                                .value(self.history_filter_mode.clone())
                                                                .items(history_mode_items.clone())
                                                                .on_change(ctx.link().callback(Msg::HistoryFilterModeChanged))}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Recoverable")}</div>
                                                            {Combobox::new()
                                                                .style("width", "100%")
                                                                .editable(false)
                                                                .value(self.history_filter_recoverable.clone())
                                                                .items(yes_no_items.clone())
                                                                .on_change(ctx.link().callback(Msg::HistoryFilterRecoverableChanged))}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Snapshot Contains")}</div>
                                                            {Field::new()
                                                                .style("width", "100%")
                                                                .value(self.history_filter_text.clone())
                                                                .placeholder(tr!("snapshot text"))
                                                                .on_input(ctx.link().callback(Msg::HistoryFilterChanged))
                                                                .with_trigger(
                                                                    Trigger::new(history_filter_clear_icon).on_activate(
                                                                        ctx.link().callback(|_| Msg::HistoryFilterChanged(String::new())),
                                                                    ),
                                                                    true,
                                                                )}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Details Contains")}</div>
                                                            {Field::new()
                                                                .style("width", "100%")
                                                                .value(self.history_filter_details_text.clone())
                                                                .placeholder(tr!("error or output text"))
                                                                .on_input(ctx.link().callback(Msg::HistoryFilterDetailsChanged))}
                                                        </div>
                                                    </div>
                                                },
                                            ),
                                    )
                                } else {
                                    None
                                })
                                .with_child(
                                    DataTable::new(history_columns, self.history_view_store.clone())
                                        .class(pwt::css::FlexFit)
                                        .selection(self.history_selection.clone()),
                                ),
                        )
                        .with_optional_child(selected_output),
                )
                .into()
        } else {
            Container::new()
                .class(pwt::css::FlexFit)
                .class(pwt::css::ColorScheme::Neutral)
                .class("pwt-content-spacer-padding")
                .with_child(
                    Panel::new()
                        .border(true)
                        .class("pwt-fit")
                        .title(tr!("Metrics / History"))
                        .with_child(
                            Column::new()
                                .padding(3)
                                .with_child(
                                    Toolbar::new()
                                        .with_child(
                                            Button::new(tr!("Select Job"))
                                                .icon_class("fa fa-search")
                                                .disabled(!has_jobs)
                                                .on_activate(
                                                    ctx.link()
                                                        .callback(|_| Msg::OpenHistoryJobPicker),
                                                ),
                                        )
                                        .with_child(html! {
                                            <span style="opacity:0.85;">
                                                {format!("{}: {}", tr!("Job"), metrics_job_value)}
                                            </span>
                                        }),
                                )
                                .with_child(tr!(
                                    "Select a job from the list above to load metrics."
                                )),
                        ),
                )
                .into()
        };

        let failover_tab: Html = if let Some(id) = self
            .failover_job_id
            .clone()
            .or_else(|| self.default_job_id())
        {
            let recovery_snapshot_items: Rc<Vec<yew::AttrValue>> = Rc::new(
                self.recovery_points
                    .iter()
                    .map(|point| point.snapshot.clone().into())
                    .collect(),
            );
            let failover_ready = self.failover_action_for_selection(false);
            let failover_ready_ok = failover_ready.is_ok();
            let failback_ready = self.failback_action_for_selection(false);
            let failback_ready_ok = failback_ready.is_ok();
            let failback_source_conflict = self
                .failback_precheck
                .as_ref()
                .map(|precheck| precheck.source_guest_exists)
                .unwrap_or(false);
            let failback_full_required = self
                .failback_precheck
                .as_ref()
                .map(|precheck| precheck.full_required)
                .unwrap_or(false);
            let failback_execution_ready = self
                .failback_precheck
                .as_ref()
                .map(|precheck| {
                    (precheck.incremental || self.failback_allow_full)
                        && (!precheck.source_guest_exists
                            || (self.failback_restore_mode
                                == OffsiteFailbackRestoreMode::ReplaceOriginal
                                && self.failback_replace_source_guest))
                })
                .unwrap_or(false);
            let failback_restore_mode_items: Rc<Vec<yew::AttrValue>> =
                Rc::new(vec!["replace-original".into(), "restore-as-new".into()]);
            let failback_restore_mode_value = match self.failback_restore_mode {
                OffsiteFailbackRestoreMode::ReplaceOriginal => "replace-original",
                OffsiteFailbackRestoreMode::RestoreAsNew => "restore-as-new",
            };
            let selected_failover_record = self.selected_failover_record();
            let selected_record_active = selected_failover_record
                .as_ref()
                .map(|record| record.lifecycle == OffsiteFailoverLifecycle::Active)
                .unwrap_or(false);
            let selected_job_suspended = self
                .selected_failover_job()
                .map(|job| job.status.suspended)
                .unwrap_or(false);
            let selected_record_text = selected_failover_record
                .as_ref()
                .map(|record| {
                    format!(
                        "{} {} ({})",
                        tr!("VMID"),
                        record.recovery_vmid,
                        record
                            .current_name
                            .clone()
                            .or_else(|| record.recovered_name.clone())
                            .unwrap_or_else(|| tr!("unnamed"))
                    )
                })
                .unwrap_or_else(|| tr!("No promoted guest selected"));
            let failover_hint = if self.failover_running {
                tr!("Starting failover task, check the Tasks menu for live status.")
            } else if self.recovery_points_loading {
                tr!("Loading recovery snapshots from the target.")
            } else if failover_ready_ok {
                tr!("Pick a recoverable snapshot and click Promote.")
            } else {
                match failover_ready.as_ref() {
                    Ok(_) => String::new(),
                    Err(err) => format!("{}: {err}", tr!("Failover not ready")),
                }
            };
            let failback_precheck_status = self.failback_precheck.as_ref().map(|precheck| {
                let icon = if precheck.source_guest_exists || precheck.full_required {
                    "fa fa-exclamation-triangle warning"
                } else if precheck.incremental {
                    "fa fa-check good"
                } else {
                    "fa fa-exclamation-triangle warning"
                };
                let summary = if precheck.full_required {
                    tr!("A full failback transfer is required.")
                } else if precheck.source_guest_exists {
                    tr!("Original source guest needs an explicit replacement decision.")
                } else if precheck.incremental {
                    tr!("Incremental failback is available.")
                } else {
                    tr!("Incremental failback is not available.")
                };
                html! {
                    <div style="margin-top:8px;">
                        <i class={icon}></i>
                        {" "}
                        <b>{summary}</b>
                        <div style="opacity:0.8; margin-top:4px;">
                            {precheck.message.clone()}
                        </div>
                        {
                            precheck.common_snapshot.as_ref().map(|snapshot| html! {
                                <div style="opacity:0.8; margin-top:4px;">
                                    {format!("{}: {snapshot}", tr!("Common snapshot"))}
                                </div>
                            }).unwrap_or_default()
                        }
                    </div>
                }
            });
            let recovery_panel: Html = Panel::new()
                .border(true)
                .style("height", "100%")
                .title(tr!("Promote / Restore"))
                .with_child(
                    Toolbar::new()
                        .with_child(
                            Button::new(tr!("Select Job"))
                                .icon_class("fa fa-search")
                                .disabled(!has_jobs || self.failover_running)
                                .on_activate(ctx.link().callback(|_| Msg::OpenFailoverJobPicker)),
                        )
                        .with_child(
                            html! {<span style="opacity:0.85;">{format!("{}: {}", tr!("Job"), failover_job_value.clone())}</span>},
                        )
                        .with_flex_spacer()
                        .with_child(
                            Button::refresh(self.recovery_points_loading || self.failover_running)
                                .on_activate({
                                    let id = id.clone();
                                    let link = ctx.link().clone();
                                    move |_| {
                                        link.send_message(Msg::OpenFailover(id.clone().into()))
                                    }
                                }),
                        ),
                )
                .with_child(html! {
                    <div style="padding: 0 12px 8px; opacity: 0.82;">
                        {tr!("Select a recovery point, then promote it on the target node. VMID and name are generated automatically unless advanced overrides are enabled.")}
                    </div>
                })
                .with_child(
                    InputPanel::new()
                        .padding(3)
                        .with_field(
                            tr!("Recovery Snapshot"),
                            Combobox::new()
                                .key(format!("{id}-recovery-point"))
                                .placeholder(if self.recovery_points_loading {
                                    tr!("Loading...")
                                } else {
                                    tr!("Select recovery point")
                                })
                                .editable(true)
                                .value(self.failover_snapshot_input.clone())
                                .items(recovery_snapshot_items)
                                .disabled(
                                    self.recovery_points_loading
                                        || self.recovery_points.is_empty()
                                        || self.failover_running,
                                )
                                .on_change(ctx.link().callback(Msg::UpdateFailoverSnapshot)),
                        )
                        .with_large_field(
                            tr!("Start Guest"),
                            Checkbox::new()
                                .box_label(tr!("Start guest after promote"))
                                .checked(self.failover_start_guest)
                                .on_change(ctx.link().callback(Msg::UpdateFailoverStart)),
                        ),
                )
                .with_child(
                    Toolbar::new()
                        .with_child(
                            Button::new(tr!("Promote"))
                                .icon_class("fa fa-bolt")
                                .disabled(!failover_ready_ok || self.failover_running)
                                .on_activate({
                                    let link = ctx.link().clone();
                                    let start_guest = self.failover_start_guest;
                                    move |_| link.send_message(Msg::RequestFailover(start_guest))
                                }),
                        )
                        .with_child(
                            Button::new(tr!("Promote + Start"))
                                .icon_class("fa fa-play-circle")
                                .disabled(!failover_ready_ok || self.failover_running)
                                .on_activate({
                                    let link = ctx.link().clone();
                                    move |_| link.send_message(Msg::RequestFailover(true))
                                }),
                        ),
                )
                .with_child(
                    Toolbar::new()
                        .with_child(
                            Button::new(if self.failover_advanced_expanded {
                                tr!("Hide Advanced")
                            } else {
                                tr!("Advanced Options")
                            })
                            .icon_class("fa fa-cog")
                            .on_activate(ctx.link().callback(|_| Msg::ToggleFailoverAdvanced)),
                        )
                        .with_child(
                            Button::new(tr!("Reset Defaults"))
                                .icon_class("fa fa-refresh")
                                .disabled(!self.failover_form_dirty)
                                .on_activate(ctx.link().callback(|_| Msg::ResetFailoverDefaults)),
                        ),
                )
                .with_optional_child(if self.failover_advanced_expanded {
                    Some(
                        InputPanel::new()
                            .padding(3)
                            .with_field(
                                tr!("Recovery VMID"),
                                Field::new()
                                    .value(self.failover_vmid_input.clone())
                                    .placeholder(tr!("e.g. 500"))
                                    .on_change(ctx.link().callback(Msg::UpdateFailoverVmid)),
                            )
                            .with_field(
                                tr!("Recovered Name"),
                                Field::new()
                                    .value(self.failover_name_input.clone())
                                    .placeholder(tr!("optional"))
                                    .on_change(ctx.link().callback(Msg::UpdateFailoverName)),
                            ),
                    )
                } else {
                    None
                })
                .with_child(html! {
                    <div style="padding: 0 12px 12px;">
                        <div style="opacity: 0.8;">{failover_hint}</div>
                        {
                            self.failover_last_task.as_ref().map(|task| html! {
                                <div style="margin-top: 4px; opacity: 0.8;">
                                    {format!("{}: {task}", tr!("Last failover task"))}
                                </div>
                            }).unwrap_or_default()
                        }
                    </div>
                })
                .into();
            let failback_panel: Html = Panel::new()
                .border(true)
                .style("height", "100%")
                .title(tr!("Promoted Guests / Failback"))
                .with_child(
                    Toolbar::new()
                        .with_child(Fa::new("undo"))
                        .with_child(html! {
                            <span style="opacity:0.85;">{selected_record_text}</span>
                        })
                        .with_flex_spacer()
                        .with_child(
                            Button::refresh(self.failover_records_loading || self.failback_running)
                                .on_activate({
                                    let id = id.clone();
                                    let link = ctx.link().clone();
                                    move |_| {
                                        link.send_message(Msg::OpenFailover(id.clone().into()))
                                    }
                                }),
                        ),
                )
                .with_child(html! {
                    <div style="padding: 0 12px 8px; opacity: 0.82;">
                        {tr!("Select a promoted guest, choose how to restore it on the source, then run a readiness check. The promoted disk state is snapshotted automatically when failback starts.")}
                        {
                            selected_failover_record.as_ref().map(|record| html! {
                                <div style="margin-top:6px;">
                                    {format!(
                                        "{}: {} | {}: {} | {}: {} | {}: {}",
                                        tr!("Lifecycle"),
                                        failover_lifecycle_text(record.lifecycle),
                                        tr!("Target VM"),
                                        guest_state_text(record.target_guest_state),
                                        tr!("Source VM"),
                                        guest_state_text(record.source_guest_state),
                                        tr!("Lineage"),
                                        failback_lineage_text(record.lineage),
                                    )}
                                </div>
                            }).unwrap_or_default()
                        }
                    </div>
                })
                .with_child(
                    InputPanel::new()
                        .padding(3)
                        .with_field(
                            tr!("Restore Mode"),
                            Combobox::new()
                                .editable(false)
                                .value(failback_restore_mode_value)
                                .items(failback_restore_mode_items)
                                .on_change(ctx.link().callback(Msg::UpdateFailbackRestoreMode)),
                        )
                        .with_field(
                            tr!("Source Restore VMID"),
                            Field::new()
                                .value(self.failback_restore_vmid_input.clone())
                                .on_change(ctx.link().callback(Msg::UpdateFailbackRestoreVmid)),
                        )
                        .with_large_field(
                            tr!("Target Cleanup"),
                            Checkbox::new()
                                .box_label(tr!("Remove the stopped promoted guest after a successful source restore. Replication remains suspended while it is retained."))
                                .checked(self.failback_cleanup_target)
                                .on_change(ctx.link().callback(Msg::UpdateFailbackCleanupTarget)),
                        ),
                )
                .with_optional_child(
                    (failback_source_conflict
                        && self.failback_restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal)
                        .then(|| {
                            InputPanel::new().padding(3).with_large_field(
                                tr!("Source Guest Conflict"),
                                Checkbox::new()
                                    .box_label(format!(
                                        "{} {} {}",
                                        tr!("Replace existing original source guest"),
                                        self.failback_restore_vmid_input,
                                        tr!("after the precheck succeeds (destructive)."),
                                    ))
                                    .checked(self.failback_replace_source_guest)
                                    .on_change(
                                        ctx.link().callback(Msg::UpdateFailbackReplaceSource),
                                    ),
                            )
                        }),
                )
                .with_optional_child(failback_full_required.then(|| {
                    InputPanel::new().padding(3).with_large_field(
                        tr!("Full Transfer"),
                        Checkbox::new()
                            .box_label(tr!("Proceed with a full ZFS transfer when incremental lineage is unavailable."))
                            .checked(self.failback_allow_full)
                            .on_change(ctx.link().callback(Msg::UpdateFailbackAllowFull)),
                    )
                }))
                .with_child(
                    Toolbar::new()
                        .with_child(
                            Button::new(tr!("Failback Precheck"))
                                .icon_class("fa fa-search")
                                .disabled(
                                    !failback_ready_ok
                                        || self.failover_running
                                        || self.failback_running,
                                )
                                .on_activate(ctx.link().callback(|_| Msg::FailbackPrecheck)),
                        )
                        .with_child(
                            Button::new(tr!("Failback"))
                                .icon_class("fa fa-undo")
                                .disabled(
                                    !failback_ready_ok
                                        || self.failover_running
                                        || self.failback_running
                                        || !failback_execution_ready,
                                )
                                .on_activate({
                                    let link = ctx.link().clone();
                                    let start_guest = self.failover_start_guest;
                                    move |_| link.send_message(Msg::RequestFailback(start_guest))
                                }),
                        )
                        .with_child(
                            Button::new(tr!("Failback + Start"))
                                .icon_class("fa fa-play-circle")
                                .disabled(
                                    !failback_ready_ok
                                        || self.failover_running
                                        || self.failback_running
                                        || !failback_execution_ready,
                                )
                                .on_activate({
                                    let link = ctx.link().clone();
                                    move |_| link.send_message(Msg::RequestFailback(true))
                                }),
                        ),
                )
                .with_child(html! {
                    <div style="padding: 0 12px 12px;">
                        {failback_precheck_status.unwrap_or_default()}
                        {
                            self.failback_last_task.as_ref().map(|task| html! {
                                <div style="margin-top: 4px; opacity: 0.8;">
                                    {format!("{}: {task}", tr!("Last failback task"))}
                                </div>
                            }).unwrap_or_default()
                        }
                    </div>
                })
                .into();
            let snapshot_states_panel: Html = Panel::new()
                .border(true)
                .style("height", "100%")
                .title(tr!("Recovery Snapshot States"))
                .with_child(html! {
                    <div style="padding: 0 12px 4px; opacity: 0.8;">
                        {tr!("Transfer mode does not block recovery; any listed recoverable snapshot can be promoted.")}
                    </div>
                })
                .with_child(render_recovery_snapshot_state_tiles(&self.recovery_points))
                .into();
            let recovery_snapshots_panel_height = if self.recovery_filters_expanded {
                "440px"
            } else {
                "320px"
            };

            Container::new()
                .class(pwt::css::FlexFit)
                .class(pwt::css::ColorScheme::Neutral)
                .class("pwt-content-spacer-padding")
                .with_child(
                    Column::new()
                        .class(pwt::css::FlexFit)
                        .gap(3)
                        .with_child(
                            html! {
                                <div style="display:grid; gap:12px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); align-items:stretch;">
                                    {recovery_panel}
                                    {snapshot_states_panel}
                                    {failback_panel}
                                </div>
                            },
                        )
                        .with_child(
                            Panel::new()
                                .border(true)
                                .title(tr!("Recoverable Snapshots"))
                                .style("height", recovery_snapshots_panel_height)
                                .class(pwt::css::FlexFit)
                                .with_child(
                                    Toolbar::new()
                                        .border_bottom(true)
                                        .with_child(
                                            Button::new(format!(
                                                "{} ({recovery_filters_active})",
                                                tr!("Clear Filter")
                                            ))
                                            .disabled(recovery_filters_active == 0)
                                            .on_activate(ctx.link().callback(|_| Msg::ClearRecoveryFilters)),
                                        )
                                        .with_child(
                                            Button::new(tr!("Filter"))
                                                .icon_class("fa fa-filter")
                                                .on_activate(
                                                    ctx.link().callback(|_| Msg::ToggleRecoveryFilters),
                                                ),
                                        )
                                        .with_child(
                                            Button::new(tr!("Delete Snapshot"))
                                                .icon_class("fa fa-trash")
                                                .disabled(
                                                    self.recovery_delete_running
                                                        || self.recovery_points_loading
                                                        || self.recovery_points.is_empty()
                                                        || self.recovery_selection.is_empty(),
                                                )
                                                .on_activate(
                                                    ctx.link()
                                                        .callback(|_| Msg::OpenDeleteRecoverySnapshot),
                                                ),
                                        )
                                        .with_flex_spacer()
                                        .with_child(
                                            html! {<span style="opacity:0.75;">{format!("{}: {recovery_visible}/{}", tr!("Visible"), self.recovery_points.len())}</span>},
                                        ),
                                )
                                .with_optional_child(if self.recovery_filters_expanded {
                                    Some(
                                        Panel::new()
                                            .border_bottom(true)
                                            .with_child(
                                                html! {
                                                    <div style="display:grid; gap:12px; padding: 12px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));">
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Mode")}</div>
                                                            {Combobox::new()
                                                                .style("width", "100%")
                                                                .editable(false)
                                                                .value(self.recovery_filter_mode.clone())
                                                                .items(history_mode_items.clone())
                                                                .on_change(ctx.link().callback(Msg::RecoveryFilterModeChanged))}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Recoverable")}</div>
                                                            {Combobox::new()
                                                                .style("width", "100%")
                                                                .editable(false)
                                                                .value(self.recovery_filter_recoverable.clone())
                                                                .items(yes_no_items.clone())
                                                                .on_change(ctx.link().callback(Msg::RecoveryFilterRecoverableChanged))}
                                                        </div>
                                                        <div style="display:flex; flex-direction:column; gap:6px;">
                                                            <div style="opacity:0.85;">{tr!("Snapshot Contains")}</div>
                                                            {Field::new()
                                                                .style("width", "100%")
                                                                .value(self.recovery_filter_text.clone())
                                                                .placeholder(tr!("snapshot text"))
                                                                .on_input(ctx.link().callback(Msg::RecoveryFilterChanged))
                                                                .with_trigger(
                                                                    Trigger::new(recovery_filter_clear_icon).on_activate(
                                                                        ctx.link().callback(|_| Msg::RecoveryFilterChanged(String::new())),
                                                                    ),
                                                                    true,
                                                                )}
                                                        </div>
                                                    </div>
                                                },
                                            ),
                                    )
                                } else {
                                    None
                                })
                                .with_child(
                                    DataTable::new(
                                        self.recovery_columns.clone(),
                                        self.recovery_view_store.clone(),
                                    )
                                        .class(pwt::css::FlexFit)
                                        .selection(self.recovery_selection.clone()),
                                ),
                        )
                        .with_child(
                            Panel::new()
                                .border(true)
                                .title(tr!("Promoted Guests"))
                                .style("height", "260px")
                                .class(pwt::css::FlexFit)
                                .with_child(
                                    Toolbar::new()
                                        .border_bottom(true)
                                        .with_child(html! {
                                            <span style="opacity:0.75;">
                                                {format!("{}: {}", tr!("Records"), self.failover_records.len())}
                                            </span>
                                        })
                                        .with_child(
                                            Button::new(tr!("Archive / Abandon"))
                                                .icon_class("fa fa-archive")
                                                .disabled(
                                                    !selected_record_active
                                                        || self.failback_running,
                                                )
                                                .on_activate(
                                                    ctx.link().callback(|_| Msg::RequestAbandonFailover),
                                                ),
                                        )
                                        .with_child(
                                            Button::new(tr!("Resume Replication"))
                                                .icon_class("fa fa-play")
                                                .disabled(!selected_job_suspended)
                                                .on_activate(
                                                    ctx.link().callback(|_| Msg::ResumeReplication),
                                                ),
                                        )
                                        .with_flex_spacer()
                                        .with_child(
                                            html! {<span style="opacity:0.75;">{tr!("Select a record for failback.")}</span>},
                                        ),
                                )
                                .with_child(
                                    DataTable::new(
                                        self.failover_record_columns.clone(),
                                        self.failover_record_store.clone(),
                                    )
                                        .class(pwt::css::FlexFit)
                                        .selection(self.failover_record_selection.clone()),
                                ),
                        ),
                )
                .into()
        } else {
            Container::new()
                .class(pwt::css::FlexFit)
                .class(pwt::css::ColorScheme::Neutral)
                .class("pwt-content-spacer-padding")
                .with_child(
                    Panel::new()
                        .border(true)
                        .class("pwt-fit")
                        .title(tr!("Failover / Restore"))
                        .with_child(
                            Column::new()
                                .padding(3)
                                .with_child(
                                    Toolbar::new()
                                        .with_child(
                                            Button::new(tr!("Select Job"))
                                                .icon_class("fa fa-search")
                                                .disabled(!has_jobs)
                                                .on_activate(
                                                    ctx.link()
                                                        .callback(|_| Msg::OpenFailoverJobPicker),
                                                ),
                                        )
                                        .with_child(html! {
                                            <span style="opacity:0.85;">
                                                {format!("{}: {}", tr!("Job"), failover_job_value)}
                                            </span>
                                        }),
                                )
                                .with_child(tr!(
                                    "Select a job from the list above to load recovery points."
                                )),
                        ),
                )
                .into()
        };

        let tabs: Html = TabPanel::new()
            .selection(self.main_tab_selection.clone())
            .class(pwt::css::FlexFit)
            .class(pwt::css::ColorScheme::Neutral)
            .with_item_builder(
                TabBarItem::new()
                    .key(TAB_JOBS)
                    .label(tr!("Jobs / Configuration"))
                    .icon_class("fa fa-database"),
                move |_| jobs_tab.clone(),
            )
            .with_item_builder(
                TabBarItem::new()
                    .key(TAB_METRICS)
                    .label(tr!("Metrics / History"))
                    .icon_class("fa fa-area-chart"),
                move |_| metrics_tab.clone(),
            )
            .with_item_builder(
                TabBarItem::new()
                    .key(TAB_FAILOVER)
                    .label(tr!("Failover / Restore"))
                    .icon_class("fa fa-bolt"),
                move |_| failover_tab.clone(),
            )
            .into();

        let run_now_feedback = self.run_now_feedback.as_ref().map(|feedback| {
            Container::new()
                .class("pwt-content-spacer-padding")
                .with_child(
                    Panel::new()
                        .border(true)
                        .title(tr!("Run Request"))
                        .with_child(
                            Toolbar::new()
                                .with_child(Fa::new("play-circle").class("pwt-color-success"))
                                .with_child(format!(
                                    "{} '{}' ({})",
                                    tr!("Started run for job"),
                                    feedback.job_id,
                                    feedback.upid
                                ))
                                .with_flex_spacer()
                                .with_child(
                                    Button::new(tr!("Open Task Log"))
                                        .icon_class("fa fa-list-alt")
                                        .on_activate(
                                            ctx.link().callback(|_| Msg::OpenRunNowTaskLog),
                                        ),
                                )
                                .with_child(
                                    Button::new(tr!("Dismiss"))
                                        .icon_class("fa fa-times")
                                        .on_activate(
                                            ctx.link().callback(|_| Msg::DismissRunNowFeedback),
                                        ),
                                ),
                        ),
                )
        });

        Column::new()
            .class(pwt::css::FlexFit)
            .with_optional_child(run_now_feedback)
            .with_child(tabs)
            .into()
    }

    fn dialog_view(
        &self,
        ctx: &LoadableComponentContext<Self>,
        view_state: &Self::ViewState,
    ) -> Option<Html> {
        match view_state {
            ViewState::Create => Some(self.create_add_dialog(ctx)),
            ViewState::Edit => self
                .selection
                .selected_key()
                .map(|key| self.create_edit_dialog(key, ctx)),
            ViewState::Remove => self.selected_job().map(|job| {
                let id = job.job.id.clone();
                let purge_target_snapshots = self.remove_purge_target_snapshots;

                let content = Column::new()
                    .gap(2)
                    .with_child(tr!("Remove off-site replication job '{0}'?", id.clone()))
                    .with_child(
                        Checkbox::new()
                            .box_label(tr!(
                                "Also purge target snapshots recorded by this job (destructive)."
                            ))
                            .checked(purge_target_snapshots)
                            .on_change(ctx.link().callback(Msg::RemovePurgeChanged)),
                    );

                let mut dialog = ConfirmDialog::default().on_confirm({
                    let link = ctx.link().clone();
                    move |_| {
                        link.send_message(Msg::Remove(id.clone().into(), purge_target_snapshots))
                    }
                });
                dialog.set_confirm_message(content);
                dialog.into()
            }),
            ViewState::DeleteRecoverySnapshot => {
                self.recovery_delete_candidate.clone().map(|snapshot| {
                    let snapshot_for_confirm = snapshot.clone();
                    let mut dialog = ConfirmDialog::default().on_confirm({
                        let link = ctx.link().clone();
                        move |_| link.send_message(Msg::DeleteRecoverySnapshot(snapshot.clone()))
                    });
                    dialog.set_confirm_message(format!(
                        "{}\n{}",
                        tr!(
                            "Delete recoverable snapshot '{0}' from target storage?",
                            snapshot_for_confirm
                        ),
                        tr!("This action cannot be undone.")
                    ));
                    dialog.into()
                })
            }
            ViewState::ConfirmAbandonFailover => self
                .selected_failover_record()
                .map(|record| {
                    let job_id = record.job_id.clone();
                    let record_id = record.record_id.clone();
                    let mut dialog = ConfirmDialog::default().dangerous(true).on_confirm({
                        let link = ctx.link().clone();
                        move |_| {
                            link.send_message(Msg::AbandonFailover(
                                job_id.clone(),
                                record_id.clone(),
                            ))
                        }
                    });
                    dialog.set_confirm_message(format!(
                        "{}\n{}",
                        tr!(
                            "Archive promoted guest VMID {vmid}?",
                            vmid = record.recovery_vmid,
                        ),
                        tr!("The audit record and target data are retained. Replication resumes only when no active promotion remains and the source guest is available.")
                    ));
                    dialog.into()
                }),
            ViewState::PickHistoryJob => Some(self.create_job_picker_dialog(
                tr!("Select Job for Metrics / History"),
                JobPickerTarget::History,
                ctx,
            )),
            ViewState::PickFailoverJob => Some(self.create_job_picker_dialog(
                tr!("Select Job for Failover / Restore"),
                JobPickerTarget::Failover,
                ctx,
            )),
            ViewState::PickRunNowJob => Some(self.create_job_picker_dialog(
                tr!("Select Job to Run Now"),
                JobPickerTarget::RunNow,
                ctx,
            )),
            ViewState::ConfirmFailback(start_guest) => self
                .failback_action_for_selection(*start_guest)
                .ok()
                .map(|action| {
                    let action_for_confirm = action.clone();
                    let mut dialog = ConfirmDialog::default().dangerous(true).on_confirm({
                        let link = ctx.link().clone();
                        move |_| {
                            link.send_message(Msg::TriggerFailback(
                                action_for_confirm.job_id.clone(),
                                action_for_confirm.request.clone(),
                            ))
                        }
                    });
                    let mut details = Vec::new();
                    if action.request.restore_mode == OffsiteFailbackRestoreMode::ReplaceOriginal {
                        details.push(tr!(
                            "Returned disks are received and validated in staging before the existing source guest is replaced."
                        ));
                    } else {
                        details.push(format!(
                            "{} {}.",
                            tr!("A new source-side guest will be registered with VMID"),
                            action.request.restore_vmid,
                        ));
                    }
                    if action.request.allow_full {
                        details.push(tr!("A full ZFS transfer will be sent because incremental lineage is unavailable."));
                    }
                    if action.request.cleanup_target {
                        details.push(tr!("The stopped promoted target guest will be removed after source registration succeeds."));
                    }
                    details.push(tr!("This action cannot be undone."));
                    dialog.set_confirm_message(format!(
                        "{}\n{}",
                        tr!(
                            "Start failback for promoted guest to source VMID {vmid}?",
                            vmid = action.request.restore_vmid,
                        ),
                        details.join("\n")
                    ));
                    dialog.into()
                }),
        }
    }
}

enum InputPanelMode {
    Create,
    Edit(String),
}

fn set_default_form_text(form_ctx: &FormContext, key: &'static str, default: &'static str) {
    if form_ctx.read().get_field_text(key).trim().is_empty() {
        form_ctx.write().set_field_value(key, default.into());
    }
}

fn format_ssh_prepare_details(result: &OffsiteSshPrepareResult) -> String {
    let mut lines = vec![
        format!("{}: {}", tr!("Source Host"), result.source_host),
        format!("{}: {}", tr!("Target Host"), result.target_host),
        String::new(),
    ];

    for step in &result.steps {
        let status = if step.ok { "OK" } else { "ERROR" };
        lines.push(format!("[{status}] {}: {}", step.name, step.message));
        if let Some(remediation) = step.remediation.as_deref() {
            lines.push(format!("  {}: {remediation}", tr!("Hint")));
        }
    }

    lines.join("\n")
}

fn render_ssh_prepare_feedback(
    result: Option<OffsiteSshPrepareResult>,
    error: Option<String>,
    details_expanded: bool,
    details_text: String,
) -> Html {
    if let Some(error) = error.as_deref() {
        return html! {
            <div style="padding-top: 6px; display: grid; gap: 4px;">
                <div class="pwt-color-error">
                    {format!("{}: {error}", tr!("Last error"))}
                </div>
                {if details_expanded && !details_text.trim().is_empty() {
                    html! {
                        <pre style="margin: 0; white-space: pre-wrap; word-break: break-word; font-family: monospace; font-size: 0.92em; border: 1px solid var(--pwt-color-border); padding: 8px; border-radius: 2px;">
                            {details_text}
                        </pre>
                    }
                } else {
                    html! {}
                }}
            </div>
        };
    }

    let Some(result) = result else {
        return html! {};
    };

    let headline = if result.ok {
        tr!("SSH preparation passed.")
    } else {
        tr!("SSH preparation found issues.")
    };
    let row_class = if result.ok {
        "pwt-color-success"
    } else {
        "pwt-color-warning"
    };
    let failed_count = result.steps.iter().filter(|step| !step.ok).count();
    let passed_count = result.steps.len().saturating_sub(failed_count);

    html! {
        <div style="padding-top: 6px; display: grid; gap: 4px;">
            <div class={row_class}>{headline}</div>
            <div style="opacity: 0.8;">
                {format!("{}: {}  {}: {}", tr!("Source Host"), result.source_host, tr!("Target Host"), result.target_host)}
            </div>
            <div style="opacity: 0.85;">
                {format!("{}: {passed_count}, {}: {failed_count}", tr!("Passed Steps"), tr!("Failed Steps"))}
            </div>
            {if failed_count > 0 {
                html! {<div class="pwt-color-warning">{tr!("Use 'View Details' for full remediation steps.")}</div>}
            } else {
                html! {}
            }}
            {if details_expanded && !details_text.trim().is_empty() {
                html! {
                    <pre style="margin: 0; white-space: pre-wrap; word-break: break-word; font-family: monospace; font-size: 0.92em; border: 1px solid var(--pwt-color-border); padding: 8px; border-radius: 2px;">
                        {details_text}
                    </pre>
                }
            } else {
                html! {}
            }}
        </div>
    }
}

fn input_panel(
    form_ctx: &FormContext,
    mode: InputPanelMode,
    on_prepare_ssh: Option<Callback<Value>>,
    on_generate_ssh_key: Option<Callback<Value>>,
    on_prepare_ssh_details: Option<Callback<()>>,
    ssh_prepare_running: bool,
    ssh_prepare_result: Option<OffsiteSshPrepareResult>,
    ssh_prepare_error: Option<String>,
    ssh_prepare_details_expanded: bool,
    ssh_prepare_last_details: String,
    ssh_keygen_running: bool,
    ssh_keygen_error: Option<String>,
    ssh_keygen_overwrite_armed: bool,
) -> Html {
    let guest_types = Rc::new(vec!["qemu".into(), "lxc".into()]);
    let is_create = matches!(mode, InputPanelMode::Create);

    if is_create {
        set_default_form_text(form_ctx, "guest-type", "qemu");
        set_default_form_text(form_ctx, "target-dataset", DEFAULT_TARGET_DATASET);
        set_default_form_text(form_ctx, "schedule", DEFAULT_JOB_SCHEDULE);
        set_default_form_text(form_ctx, "max-snapshots", DEFAULT_MAX_SNAPSHOTS);
        set_default_form_text(form_ctx, "history-limit", DEFAULT_HISTORY_LIMIT);
        set_default_form_text(form_ctx, "zfs-stream-mode", DEFAULT_ZFS_STREAM_MODE);
        set_default_form_text(form_ctx, "source-user", DEFAULT_SOURCE_USER);
        set_default_form_text(form_ctx, "target-user", DEFAULT_TARGET_USER);
        set_default_form_text(form_ctx, "ssh-private-key", DEFAULT_SSH_PRIVATE_KEY);
    }

    let source_remote = form_ctx.read().get_field_text("source-remote");
    let source_node = form_ctx.read().get_field_text("source-node");
    let guest_type = form_ctx.read().get_field_text("guest-type");
    let target_remote = form_ctx.read().get_field_text("target-remote");
    let vmid_default = form_ctx.read().get_field_text("vmid");
    let show_advanced = if is_create {
        form_ctx.read().get_field_checked("show-advanced")
    } else {
        true
    };
    let install_authorized_keys = form_ctx.read().get_field_checked("install-authorized-keys");
    let copy_key_to_source = form_ctx.read().get_field_checked("copy-key-to-source");

    let source_remote_value: yew::AttrValue = source_remote.clone().into();
    let source_node_value = if source_node.trim().is_empty() {
        None
    } else {
        Some(yew::AttrValue::from(source_node.clone()))
    };
    let target_remote_value: yew::AttrValue = target_remote.clone().into();
    let guest_type_value: yew::AttrValue = if guest_type.trim().is_empty() {
        "qemu".into()
    } else {
        guest_type.clone().into()
    };
    let vmid_default_value: Option<yew::AttrValue> = if vmid_default.trim().is_empty() {
        None
    } else {
        Some(vmid_default.into())
    };

    let mut panel = InputPanel::new().padding(4);

    match mode {
        InputPanelMode::Create => panel.add_field(
            tr!("ID"),
            Field::new()
                .name("id")
                .schema(&OFFSITE_REPLICATION_ID_SCHEMA)
                .required(true),
        ),
        InputPanelMode::Edit(id) => {
            panel.add_field(tr!("ID"), DisplayField::new().name("id").value(id));
        }
    }

    let source_remote_field = if is_create {
        RemoteSelector::new()
            .name("source-remote")
            .remote_type(RemoteType::Pve)
            .on_change({
                let form_ctx = form_ctx.clone();
                move |_| {
                    let mut form = form_ctx.write();
                    form.set_field_value("source-node", Value::Null);
                    form.set_field_value("vmid", Value::Null);
                }
            })
            .required(true)
    } else {
        RemoteSelector::new()
            .name("source-remote")
            .remote_type(RemoteType::Pve)
            .required(true)
    };

    let guest_type_field = if is_create {
        Combobox::new()
            .name("guest-type")
            .required(true)
            .editable(false)
            .items(guest_types.clone())
            .on_change({
                let form_ctx = form_ctx.clone();
                move |_| {
                    form_ctx.write().set_field_value("vmid", Value::Null);
                }
            })
    } else {
        Combobox::new()
            .name("guest-type")
            .required(true)
            .editable(false)
            .items(guest_types.clone())
    };

    let target_remote_field = if is_create {
        RemoteSelector::new()
            .name("target-remote")
            .remote_type(RemoteType::Pve)
            .on_change({
                let form_ctx = form_ctx.clone();
                move |_| {
                    form_ctx.write().set_field_value("target-node", Value::Null);
                }
            })
            .required(true)
    } else {
        RemoteSelector::new()
            .name("target-remote")
            .remote_type(RemoteType::Pve)
            .required(true)
    };

    panel = panel
        .with_field(tr!("Source Remote"), source_remote_field)
        .with_field(
            tr!("Source Node"),
            PveNodeSelector::new(source_remote_value.clone())
                .name("source-node")
                .required(true)
                .disabled(source_remote.trim().is_empty()),
        )
        .with_field(tr!("Guest Type"), guest_type_field)
        .with_field(
            tr!("Source Guest (VMID)"),
            PveGuestSelector::new(source_remote_value)
                .name("vmid")
                .node(source_node_value)
                .guest_type(guest_type_value)
                .default(vmid_default_value)
                .required(true)
                .disabled(source_remote.trim().is_empty() || source_node.trim().is_empty())
                .on_change({
                    let form_ctx = form_ctx.clone();
                    let create_auto_id = is_create;
                    move |value: Option<yew::AttrValue>| {
                        if !create_auto_id {
                            return;
                        }
                        let Some(vmid) = value.map(|value| value.to_string()) else {
                            return;
                        };
                        let mut form = form_ctx.write();
                        if form.get_field_text("id").trim().is_empty() {
                            form.set_field_value("id", format!("job-{vmid}").into());
                        }
                    }
                }),
        )
        .with_field(tr!("Target Remote"), target_remote_field)
        .with_field(
            tr!("Target Node"),
            PveNodeSelector::new(target_remote_value)
                .name("target-node")
                .required(true)
                .disabled(target_remote.trim().is_empty()),
        )
        .with_field(
            tr!("Target Dataset"),
            Field::new().name("target-dataset").required(true),
        )
        .with_field(
            tr!("Schedule"),
            Field::new()
                .name("schedule")
                .schema(&OFFSITE_REPLICATION_SCHEDULE_SCHEMA)
                .required(true),
        )
        .with_field(
            tr!("Max Snapshots"),
            Field::new()
                .name("max-snapshots")
                .schema(&OFFSITE_REPLICATION_MAXSNAP_SCHEMA)
                .required(true),
        )
        .with_field(
            tr!("History Limit"),
            Field::new()
                .name("history-limit")
                .schema(&OFFSITE_REPLICATION_HISTORY_LIMIT_SCHEMA)
                .required(true),
        )
        .with_field(
            tr!("ZFS Stream Mode"),
            Combobox::new()
                .name("zfs-stream-mode")
                .required(true)
                .editable(false)
                .items(Rc::new(vec!["auto".into(), "plain".into(), "raw".into()])),
        );

    if is_create {
        panel = panel.with_large_field(
            tr!("Advanced Options"),
            Checkbox::new()
                .name("show-advanced")
                .default(false)
                .box_label(tr!(
                    "Show advanced settings (rate limit, SSH, guest agent freeze)."
                )),
        );
    }

    if show_advanced {
        let prepare_missing_private_key = ssh_prepare_result
            .as_ref()
            .map(|result| {
                !result.ok
                    && result.steps.iter().any(|step| {
                        !step.ok && step.code.as_deref() == Some(SSH_STEP_CODE_MISSING_PRIVATE_KEY)
                    })
            })
            .unwrap_or(false);
        let show_generate_key_button = prepare_missing_private_key || ssh_keygen_overwrite_armed;
        let has_prepare_details = !ssh_prepare_last_details.trim().is_empty();
        let prepare_button = {
            let callback = on_prepare_ssh.clone();
            let form_ctx = form_ctx.clone();
            Button::new(if ssh_prepare_running {
                tr!("Prepare SSH (Running...)")
            } else {
                tr!("Prepare SSH")
            })
            .icon_class("fa fa-key")
            .button_type(ButtonType::Button)
            .disabled(ssh_prepare_running || callback.is_none())
            .on_activate(move |_| {
                if let Some(callback) = callback.clone() {
                    callback.emit(form_ctx.get_submit_data());
                }
            })
        };
        let generate_key_button = {
            let callback = on_generate_ssh_key.clone();
            let form_ctx = form_ctx.clone();
            Button::new(if ssh_keygen_running {
                tr!("Generate Key (Running...)")
            } else if ssh_keygen_overwrite_armed {
                tr!("Overwrite Key")
            } else {
                tr!("Generate Key")
            })
            .icon_class("fa fa-cog")
            .button_type(ButtonType::Button)
            .disabled(ssh_prepare_running || ssh_keygen_running || callback.is_none())
            .on_activate(move |_| {
                if let Some(callback) = callback.clone() {
                    callback.emit(form_ctx.get_submit_data());
                }
            })
        };
        let view_details_button = {
            let callback = on_prepare_ssh_details.clone();
            Button::new(if ssh_prepare_details_expanded {
                tr!("Hide Details")
            } else {
                tr!("View Details")
            })
            .icon_class("fa fa-list-alt")
            .button_type(ButtonType::Button)
            .disabled(callback.is_none() || !has_prepare_details)
            .on_activate(move |_| {
                if let Some(callback) = callback.clone() {
                    callback.emit(());
                }
            })
        };

        panel = panel
            .with_field(
                tr!("Rate Limit (MiB/s)"),
                Field::new()
                    .name("rate-limit-mib")
                    .schema(&RATE_LIMIT_MIB_SCHEMA),
            )
            .with_field(
                tr!("Source SSH User"),
                Field::new().name("source-user").required(true),
            )
            .with_field(
                tr!("Target SSH User"),
                Field::new().name("target-user").required(true),
            )
            .with_field(
                tr!("SSH Private Key"),
                Field::new().name("ssh-private-key").required(true),
            )
            .with_large_custom_child(
                Column::new()
                    .key("ssh-setup-assistant")
                    .gap(1)
                    .with_child(
                        Panel::new().border(true).with_child(
                            Column::new()
                                .padding(2)
                                .gap(1)
                                .with_child(
                                    Container::new()
                                        .class("pwt-font-title-small")
                                        .with_child(tr!("SSH Setup Assistant")),
                                )
                                .with_child(
                                    Checkbox::new()
                                        .name("install-authorized-keys")
                                        .default(true)
                                        .checked(install_authorized_keys)
                                        .box_label(tr!(
                                            "Install/verify public key in authorized_keys on source and target."
                                        )),
                                )
                                .with_child(
                                    Checkbox::new()
                                        .name("copy-key-to-source")
                                        .default(false)
                                        .checked(copy_key_to_source)
                                        .box_label(tr!(
                                            "Copy keypair to source host for source->target pve-zsync hop."
                                        )),
                                )
                                .with_child(
                                    Row::new()
                                        .gap(2)
                                        .with_child(prepare_button)
                                        .with_optional_child(
                                            show_generate_key_button.then_some(generate_key_button),
                                        )
                                        .with_child(view_details_button)
                                        .with_flex_spacer(),
                                )
                                .with_optional_child(ssh_keygen_error.as_ref().map(|message| {
                                    let class = if message.starts_with("SSH key prepared:") {
                                        "pwt-color-success"
                                    } else {
                                        "pwt-color-warning"
                                    };
                                    html! {
                                        <div class={class}>{message.clone()}</div>
                                    }
                                }))
                                .with_child(render_ssh_prepare_feedback(
                                    ssh_prepare_result,
                                    ssh_prepare_error,
                                    ssh_prepare_details_expanded,
                                    ssh_prepare_last_details,
                                )),
                        ),
                    )
            )
            .with_large_field(
                tr!("QEMU Guest Agent Freeze"),
                Checkbox::new()
                    .name("qga-fsfreeze")
                    .default(false)
                    .box_label(tr!("Try fsfreeze/thaw around sync for QEMU guests.")),
            )
            .with_large_field(
                tr!("Disable Job"),
                Checkbox::new()
                    .name("disable")
                    .default(false)
                    .box_label(tr!("Disable scheduling for this job.")),
            )
            .with_large_field(tr!("Comment"), Field::new().name("comment"));
    }

    panel.into()
}

fn normalize_u32_field(data: &mut Value, key: &str) -> Result<(), Error> {
    if let Some(value) = data.get_mut(key) {
        if let Some(text) = value.as_str() {
            let parsed = text
                .trim()
                .parse::<u32>()
                .map_err(|_| anyhow::format_err!("'{key}' must be a valid number"))?;
            *value = Value::from(parsed);
        }
    }
    Ok(())
}

fn normalize_u64_field(data: &mut Value, key: &str) -> Result<(), Error> {
    if let Some(value) = data.get_mut(key) {
        if let Some(text) = value.as_str() {
            let parsed = text
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow::format_err!("'{key}' must be a valid number"))?;
            *value = Value::from(parsed);
        }
    }
    Ok(())
}

fn copy_alias_key(data: &mut Value, from: &str, to: &str) {
    if data.get(to).is_none() {
        if let Some(value) = data.get(from).cloned() {
            data[to] = value;
        }
    }
}

fn normalize_job_form_aliases(data: &mut Value) {
    let alias_pairs = [
        ("source_remote", "source-remote"),
        ("source_node", "source-node"),
        ("guest_type", "guest-type"),
        ("target_remote", "target-remote"),
        ("target_node", "target-node"),
        ("target_dataset", "target-dataset"),
        ("max_snapshots", "max-snapshots"),
        ("history_limit", "history-limit"),
        ("rate_limit_mib", "rate-limit-mib"),
        ("zfs_stream_mode", "zfs-stream-mode"),
        ("source_user", "source-user"),
        ("target_user", "target-user"),
        ("ssh_private_key", "ssh-private-key"),
        ("qga_fsfreeze", "qga-fsfreeze"),
    ];

    for (from, to) in alias_pairs {
        copy_alias_key(data, from, to);
    }
}

fn ensure_job_form_default(data: &mut Value, key: &str, default: &str) {
    let needs_default = data
        .get(key)
        .and_then(Value::as_str)
        .map(|value| value.trim().is_empty())
        .unwrap_or(true);

    if needs_default {
        data[key] = Value::from(default);
    }
}

fn ensure_job_form_bool(data: &mut Value, key: &str, default: bool) {
    if data.get(key).and_then(Value::as_bool).is_none() {
        data[key] = Value::from(default);
    }
}

fn parse_ssh_prepare_request(form_data: Value) -> Result<OffsiteSshPrepareRequest, Error> {
    let mut data = delete_empty_values(&form_data, &[], true);
    normalize_job_form_aliases(&mut data);

    ensure_job_form_default(&mut data, "source-user", DEFAULT_SOURCE_USER);
    ensure_job_form_default(&mut data, "target-user", DEFAULT_TARGET_USER);
    ensure_job_form_default(&mut data, "ssh-private-key", DEFAULT_SSH_PRIVATE_KEY);
    ensure_job_form_bool(&mut data, "install-authorized-keys", true);
    ensure_job_form_bool(&mut data, "copy-key-to-source", false);

    normalize_u32_field(&mut data, "vmid")?;
    let mut request: OffsiteSshPrepareRequest = serde_json::from_value(data)?;

    request.source_remote = request.source_remote.trim().to_string();
    request.source_node = request.source_node.trim().to_string();
    request.target_remote = request.target_remote.trim().to_string();
    request.target_node = request.target_node.trim().to_string();
    request.source_user = request.source_user.trim().to_string();
    request.target_user = request.target_user.trim().to_string();
    request.ssh_private_key = request.ssh_private_key.trim().to_string();

    if request.source_remote.is_empty()
        || request.source_node.is_empty()
        || request.target_remote.is_empty()
        || request.target_node.is_empty()
    {
        bail!("source remote/node and target remote/node are required");
    }

    if request.source_user.is_empty()
        || request.target_user.is_empty()
        || request.ssh_private_key.is_empty()
    {
        bail!("source user, target user and ssh private key are required");
    }

    Ok(request)
}

fn parse_job_form(
    form_ctx: FormContext,
    fallback_id: Option<&str>,
) -> Result<OffsiteReplicationJob, Error> {
    let data = form_ctx.get_submit_data();
    let mut data = delete_empty_values(&data, &["rate-limit-mib", "comment"], true);
    normalize_job_form_aliases(&mut data);

    ensure_job_form_default(&mut data, "schedule", DEFAULT_JOB_SCHEDULE);
    ensure_job_form_default(&mut data, "max-snapshots", DEFAULT_MAX_SNAPSHOTS);
    ensure_job_form_default(&mut data, "history-limit", DEFAULT_HISTORY_LIMIT);
    ensure_job_form_default(&mut data, "target-dataset", DEFAULT_TARGET_DATASET);
    ensure_job_form_default(&mut data, "zfs-stream-mode", DEFAULT_ZFS_STREAM_MODE);
    ensure_job_form_default(&mut data, "source-user", DEFAULT_SOURCE_USER);
    ensure_job_form_default(&mut data, "target-user", DEFAULT_TARGET_USER);
    ensure_job_form_default(&mut data, "ssh-private-key", DEFAULT_SSH_PRIVATE_KEY);
    ensure_job_form_bool(&mut data, "qga-fsfreeze", false);
    ensure_job_form_bool(&mut data, "disable", false);

    if data
        .get("id")
        .and_then(Value::as_str)
        .map(|id| id.trim().is_empty())
        .unwrap_or(true)
    {
        if let Some(vmid) = data.get("vmid").and_then(Value::as_u64).or_else(|| {
            data.get("vmid")
                .and_then(Value::as_str)
                .and_then(|vmid| vmid.parse().ok())
        }) {
            data["id"] = Value::from(format!("job-{vmid}"));
        }
    }

    if let Some(path_id) = fallback_id {
        if let Some(submitted_id) = data.get("id").and_then(Value::as_str) {
            let submitted_id = submitted_id.trim();
            if !submitted_id.is_empty() && submitted_id != path_id {
                bail!("path ID and payload ID must match");
            }
        }

        data["id"] = Value::from(path_id);
    }

    normalize_u32_field(&mut data, "vmid")?;
    normalize_u64_field(&mut data, "max-snapshots")?;
    normalize_u64_field(&mut data, "history-limit")?;
    normalize_u64_field(&mut data, "rate-limit-mib")?;
    let mut job: OffsiteReplicationJob = serde_json::from_value(data)?;

    job.id = job.id.trim().to_string();
    if job.id.is_empty() {
        bail!("job id is required");
    }
    job.target_dataset = job.target_dataset.trim().to_string();
    if job.target_dataset.is_empty() {
        bail!("target dataset is required");
    }
    job.schedule = job.schedule.trim().to_string();
    if job.schedule.is_empty() {
        bail!("schedule is required");
    }
    job.source_user = job.source_user.trim().to_string();
    job.target_user = job.target_user.trim().to_string();
    job.ssh_private_key = job.ssh_private_key.trim().to_string();
    if job.source_user.is_empty() || job.target_user.is_empty() || job.ssh_private_key.is_empty() {
        bail!("source user, target user and ssh private key are required");
    }

    Ok(job)
}

async fn create_job(form_ctx: FormContext) -> Result<(), Error> {
    let job = parse_job_form(form_ctx, None)?;
    http_post(BASE_URL, Some(serde_json::to_value(job)?)).await
}

async fn update_job(id: String, form_ctx: FormContext) -> Result<(), Error> {
    let job = parse_job_form(form_ctx, Some(id.as_str()))?;
    let path = format!("{BASE_URL}/{}", percent_encode_component(&id));
    http_put(path, Some(serde_json::to_value(job)?)).await
}

fn zfs_stream_mode_text(mode: OffsiteZfsStreamMode) -> &'static str {
    match mode {
        OffsiteZfsStreamMode::Auto => "auto",
        OffsiteZfsStreamMode::Plain => "plain",
        OffsiteZfsStreamMode::Raw => "raw",
    }
}

fn guest_type_text(guest_type: GuestType) -> &'static str {
    match guest_type {
        GuestType::Qemu => "qemu",
        GuestType::Lxc => "lxc",
    }
}

fn status_text(item: &OffsiteReplicationJobStatus) -> String {
    if item.status.running {
        return tr!("Running");
    }
    if item.job.disable {
        return tr!("Disabled");
    }
    if item.status.suspended {
        return format!(
            "{}: {}",
            tr!("Suspended"),
            item.status
                .suspend_reason
                .as_deref()
                .unwrap_or("active recovery workflow")
        );
    }
    if let Some(error) = item.status.last_error.as_ref() {
        return format!("{}: {error}", tr!("Error"));
    }
    tr!("OK")
}

fn job_matches_filter(item: &OffsiteReplicationJobStatus, filter: &str) -> bool {
    let haystack = format!(
        "{} {} {} {} {} {} {} {} {} {}",
        item.job.id,
        guest_type_text(item.job.guest_type),
        item.job.vmid,
        item.job.source_remote,
        item.job.source_node,
        item.job.target_remote,
        item.job.target_node,
        item.job.target_dataset,
        item.job.schedule,
        zfs_stream_mode_text(item.job.zfs_stream_mode),
    )
    .to_ascii_lowercase();

    haystack.contains(filter)
}

fn history_columns_with_recoverable(
    recoverable_snapshots: HashSet<String>,
) -> Rc<Vec<DataTableHeader<OffsiteReplicationRun>>> {
    let recoverable_snapshots = Rc::new(recoverable_snapshots);
    Rc::new(vec![
        DataTableColumn::new(tr!("Ended"))
            .width("150px")
            .render(|run: &OffsiteReplicationRun| render_epoch_short(run.end_time).into())
            .sort_order(true)
            .into(),
        DataTableColumn::new(tr!("Result"))
            .width("90px")
            .render(|run: &OffsiteReplicationRun| {
                if run.success {
                    tr!("OK").into()
                } else {
                    tr!("Error").into()
                }
            })
            .into(),
        DataTableColumn::new(tr!("Mode"))
            .width("110px")
            .render(|run: &OffsiteReplicationRun| {
                run.transfer_mode
                    .clone()
                    .unwrap_or_else(|| "-".to_string())
                    .into()
            })
            .into(),
        DataTableColumn::new(tr!("Duration"))
            .width("100px")
            .render(|run: &OffsiteReplicationRun| format_duration_human(run.duration as f64).into())
            .into(),
        DataTableColumn::new(tr!("Estimated"))
            .width("110px")
            .render(|run: &OffsiteReplicationRun| match run.estimated_bytes {
                Some(bytes) => HumanByte::from(bytes).to_string().into(),
                None => "-".into(),
            })
            .into(),
        DataTableColumn::new(tr!("Sent"))
            .width("110px")
            .render(|run: &OffsiteReplicationRun| {
                match run.transferred_bytes.or(run.estimated_bytes) {
                    Some(bytes) => HumanByte::from(bytes).to_string().into(),
                    None => "-".into(),
                }
            })
            .into(),
        DataTableColumn::new(tr!("Recoverable"))
            .width("110px")
            .render({
                let recoverable_snapshots = recoverable_snapshots.clone();
                move |run: &OffsiteReplicationRun| match run.snapshot.as_deref() {
                    Some(snapshot) if recoverable_snapshots.contains(snapshot) => {
                        html! {<span class="pwt-color-success">{tr!("Yes")}</span>}.into()
                    }
                    Some(_) => html! {<span class="pwt-color-warning">{tr!("No")}</span>}.into(),
                    None => "-".into(),
                }
            })
            .into(),
        DataTableColumn::new(tr!("Snapshot"))
            .flex(3)
            .render(|run: &OffsiteReplicationRun| {
                run.snapshot
                    .clone()
                    .unwrap_or_else(|| "-".to_string())
                    .into()
            })
            .into(),
        DataTableColumn::new(tr!("Details"))
            .flex(2)
            .render(|run: &OffsiteReplicationRun| {
                run.error
                    .clone()
                    .unwrap_or_else(|| {
                        run.output
                            .lines()
                            .find(|line| !line.trim().is_empty())
                            .unwrap_or("-")
                            .to_string()
                    })
                    .into()
            })
            .into(),
    ])
}

fn render_jobs_overview(jobs: &[OffsiteReplicationJobStatus]) -> Html {
    let total_jobs = jobs.len();
    let enabled_jobs = jobs.iter().filter(|job| !job.job.disable).count();
    let disabled_jobs = total_jobs.saturating_sub(enabled_jobs);
    let running_jobs = jobs.iter().filter(|job| job.status.running).count();
    let healthy_jobs = jobs
        .iter()
        .filter(|job| job.status.last_error.is_none())
        .count();
    let error_jobs = jobs
        .iter()
        .filter(|job| {
            job.status.last_error.is_some() || job.status.failure_count.unwrap_or_default() > 0
        })
        .count();
    let total_failures = jobs
        .iter()
        .map(|job| job.status.failure_count.unwrap_or_default())
        .sum::<u64>();
    let total_last_transfer = jobs
        .iter()
        .filter_map(|job| job.status.last_transfer_bytes)
        .sum::<u64>();
    let next_run = jobs.iter().filter_map(|job| job.status.next_run).min();
    let total_runs = jobs
        .iter()
        .map(|job| job.status.run_count.unwrap_or_default())
        .sum::<u64>();
    let total_success = jobs
        .iter()
        .map(|job| {
            let runs = job.status.run_count.unwrap_or_default();
            let failed = job.status.failure_count.unwrap_or_default();
            runs.saturating_sub(failed)
        })
        .sum::<u64>();
    let success_rate = if total_runs == 0 {
        "-".to_string()
    } else {
        format!("{:.0}%", (total_success as f64 / total_runs as f64) * 100.0)
    };

    html! {
        <div style="display: grid; gap: 12px; padding: 0 0 12px;">
            <div style="display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));">
                {summary_card(
                    tr!("Job Health"),
                    vec![
                        summary_row(tr!("Jobs"), "fa-database", format!("{total_jobs} configured")),
                        summary_row(tr!("Enabled"), "fa-check-square-o", format!("{enabled_jobs} enabled")),
                        summary_row(tr!("Running"), "fa-play-circle", format!("{running_jobs} running now")),
                        summary_row(tr!("Healthy"), "fa-heartbeat", format!("{healthy_jobs} without last error")),
                        summary_row(tr!("Failures"), "fa-exclamation-triangle", format!("{total_failures} accumulated failures")),
                    ],
                )}
                {summary_card(
                    tr!("Schedule / Transfer"),
                    vec![
                        summary_row(
                            tr!("Next Run"),
                            "fa-clock-o",
                            next_run.map(render_epoch_short).unwrap_or_else(|| "-".to_string()),
                        ),
                        summary_row(tr!("Last Transfer"), "fa-exchange", HumanByte::from(total_last_transfer).to_string()),
                        summary_row(tr!("Success Rate"), "fa-line-chart", format!("{success_rate} ({total_success}/{total_runs})")),
                    ],
                )}
            </div>
            <div style="display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));">
                {summary_list_card(
                    tr!("Job Status Breakdown"),
                    vec![
                        summary_list_tile("check", Some("pwt-color-success"), tr!("Enabled"), enabled_jobs),
                        summary_list_tile("ban", None, tr!("Disabled"), disabled_jobs),
                        summary_list_tile("play", Some("pwt-color-success"), tr!("Running"), running_jobs),
                        summary_list_tile("times-circle", Some("pwt-color-error"), tr!("Error"), error_jobs),
                        summary_list_tile("th", None, tr!("All"), total_jobs),
                    ],
                )}
                {summary_scroll_card(
                    tr!("Job Reliability"),
                    render_job_reliability_rows(jobs),
                    "220px",
                )}
            </div>
        </div>
    }
}

fn render_history_summary_cards(
    runs: &[OffsiteReplicationRun],
    recoverable_points: usize,
    selected_snapshot: Option<String>,
    recoverable_snapshots: &HashSet<&str>,
) -> Html {
    if runs.is_empty() {
        return html! {};
    }

    let success_count = runs.iter().filter(|run| run.success).count();
    let failure_count = runs.len().saturating_sub(success_count);
    let avg_duration =
        runs.iter().map(|run| run.duration).sum::<i64>() as f64 / runs.len().max(1) as f64;
    let total_bytes = runs
        .iter()
        .filter_map(|run| run.transferred_bytes)
        .sum::<u64>();
    let success_rate = (success_count as f64 / runs.len().max(1) as f64) * 100.0;
    let latest_snapshot = runs
        .iter()
        .find_map(|run| run.snapshot.as_deref())
        .unwrap_or("-");
    let selected_snapshot_tail = selected_snapshot
        .as_deref()
        .map(snapshot_tail)
        .unwrap_or_else(|| "-".to_string());
    let selected_recoverable = selected_snapshot
        .as_deref()
        .map(|snapshot| recoverable_snapshots.contains(snapshot))
        .unwrap_or(false);
    let selected_recoverable_text = if selected_snapshot.is_none() {
        "-".to_string()
    } else if selected_recoverable {
        tr!("Yes")
    } else {
        tr!("No")
    };
    let selected_recoverable_icon = if selected_snapshot.is_none() {
        "fa-minus"
    } else if selected_recoverable {
        "fa-check-circle"
    } else {
        "fa-times-circle"
    };

    html! {
        <div style="display: grid; gap: 12px; padding: 0 0 12px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));">
            {summary_card(
                tr!("Run Quality"),
                vec![
                    summary_row(tr!("Runs"), "fa-list-ol", format!("{} ({} ok / {} failed)", runs.len(), success_count, failure_count)),
                    summary_row(tr!("Success Rate"), "fa-line-chart", format!("{success_rate:.0}% ({success_count}/{})", runs.len())),
                    summary_row(tr!("Average Duration"), "fa-clock-o", format_duration_human(avg_duration)),
                    summary_row(tr!("Transferred"), "fa-exchange", HumanByte::from(total_bytes).to_string()),
                    summary_row(
                        tr!("Failures"),
                        "fa-exclamation-triangle",
                        match runs.iter().find(|run| !run.success).and_then(|run| run.error.as_deref()) {
                            Some(error) => error.to_string(),
                            None => tr!("No recorded failures"),
                        },
                    ),
                ],
            )}
            {summary_card(
                tr!("Recovery"),
                vec![
                    summary_row(tr!("Recoverable Points"), "fa-life-ring", recoverable_points.to_string()),
                    summary_row(tr!("Latest Snapshot"), "fa-camera", snapshot_tail(latest_snapshot)),
                    summary_row(tr!("Selected Snapshot"), "fa-map-pin", selected_snapshot_tail),
                    summary_row(tr!("Selected Recoverable"), selected_recoverable_icon, selected_recoverable_text),
                ],
            )}
        </div>
    }
}

fn render_history_trend_graphs(runs: &[OffsiteReplicationRun]) -> Html {
    if runs.is_empty() {
        return html! {};
    }

    let graphs = history_graph_series(runs, HISTORY_GRAPH_POINTS);
    let success_count = runs.iter().filter(|run| run.success).count();
    let total_count = runs.len();

    html! {
        <div style="display: grid; gap: 12px; padding: 0 0 12px; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr));">
            {render_duration_trend_graph(&graphs)}
            {render_transfer_trend_graph(&graphs)}
            {render_result_trend_graph(&graphs, success_count, total_count)}
        </div>
    }
}

fn summary_row(label: String, icon: &'static str, status: String) -> Html {
    status_row(label, icon, status).into()
}

fn summary_card(title: String, rows: Vec<Html>) -> Html {
    let mut column = Column::new().padding(2).gap(1);
    for row in rows {
        column.add_child(row);
    }

    Panel::new()
        .border(true)
        .title(title)
        .with_child(column)
        .into()
}

fn summary_scroll_card(title: String, rows: Vec<Html>, height: &'static str) -> Html {
    let mut column = Column::new().gap(0);
    for row in rows {
        column.add_child(row);
    }

    Panel::new()
        .border(true)
        .title(title)
        .with_child(
            Container::new()
                .style("height", height)
                .style("overflow-y", "auto")
                .with_child(column),
        )
        .into()
}

fn summary_list_tile(
    icon: &'static str,
    icon_color_class: Option<&'static str>,
    text: String,
    count: usize,
) -> ListTile {
    let icon_widget = match icon_color_class {
        Some(color) => Fa::new(icon).class(color),
        None => Fa::new(icon),
    };

    ListTile::new()
        .with_child(icon_widget)
        .with_child(Container::new().padding_x(2).with_child(text))
        .with_child(
            Container::new()
                .class(pwt::css::TextAlign::Right)
                .padding_end(2)
                .with_child(count.to_string()),
        )
}

fn summary_list_card(title: String, tiles: Vec<ListTile>) -> Html {
    let list = List::new(tiles.len() as u64, move |idx: u64| {
        tiles[idx as usize].clone()
    })
    .padding(2)
    .class(pwt::css::Flex::Fill)
    .grid_template_columns("auto auto 1fr auto");

    Panel::new()
        .border(true)
        .title(title)
        .with_child(list)
        .into()
}

fn render_job_reliability_rows(jobs: &[OffsiteReplicationJobStatus]) -> Vec<Html> {
    if jobs.is_empty() {
        return vec![html! {
            <div style="padding: 8px 12px; opacity: 0.8;">{tr!("No jobs available.")}</div>
        }];
    }

    let mut sorted: Vec<&OffsiteReplicationJobStatus> = jobs.iter().collect();
    sorted.sort_by(|a, b| {
        let a_failed = a.status.failure_count.unwrap_or_default();
        let b_failed = b.status.failure_count.unwrap_or_default();
        b_failed
            .cmp(&a_failed)
            .then_with(|| {
                b.status
                    .run_count
                    .unwrap_or_default()
                    .cmp(&a.status.run_count.unwrap_or_default())
            })
            .then_with(|| a.job.id.cmp(&b.job.id))
    });

    sorted
        .into_iter()
        .map(|job| {
            let runs = job.status.run_count.unwrap_or_default();
            let failed = job.status.failure_count.unwrap_or_default().min(runs);
            let success = runs.saturating_sub(failed);
            let has_last_error = job.status.last_error.is_some();

            let (ok_pct, warn_pct, err_pct, label) = if runs == 0 {
                if job.status.running {
                    (0.0, 100.0, 0.0, tr!("running"))
                } else {
                    (0.0, 0.0, 0.0, tr!("no runs"))
                }
            } else {
                let mut err_pct = (failed as f64 / runs as f64) * 100.0;
                let mut ok_pct = (success as f64 / runs as f64) * 100.0;
                let warn_pct = if has_last_error {
                    let reserve: f64 = if err_pct > 0.0 { 10.0 } else { 20.0 };
                    let reserve = reserve.min(ok_pct);
                    ok_pct -= reserve;
                    reserve
                } else {
                    0.0
                };
                if err_pct > 100.0 {
                    err_pct = 100.0;
                }
                (ok_pct, warn_pct, err_pct, format!("{success}/{runs} ok"))
            };

            let bar_style = reliability_bar_style(ok_pct, warn_pct, err_pct);
            html! {
                <div style="padding: 6px 8px;">
                    <div style="display: grid; grid-template-columns: minmax(140px, 1fr) minmax(160px, 2fr) auto; align-items: center; gap: 10px;">
                        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                            {job.job.id.clone()}
                        </div>
                        <div style={bar_style}></div>
                        <div style="opacity: 0.9; min-width: 80px; text-align: right;">{label}</div>
                    </div>
                </div>
            }
        })
        .collect()
}

fn reliability_bar_style(ok_pct: f64, warn_pct: f64, err_pct: f64) -> String {
    let total = (ok_pct + warn_pct + err_pct).clamp(0.0, 100.0);
    if total <= 0.0 {
        return "height: 18px; border-radius: 2px; background-color: var(--pwt-color-surface);"
            .to_string();
    }

    let ok_end = ok_pct.clamp(0.0, 100.0);
    let warn_end = (ok_end + warn_pct.clamp(0.0, 100.0 - ok_end)).clamp(0.0, 100.0);
    let err_end = (warn_end + err_pct.clamp(0.0, 100.0 - warn_end)).clamp(0.0, 100.0);

    format!(
        "height: 18px; border-radius: 2px; background-image: linear-gradient(to right, var(--pwt-color-success) 0% {ok_end:.2}%, var(--pwt-color-warning) {ok_end:.2}% {warn_end:.2}%, var(--pwt-color-error) {warn_end:.2}% {err_end:.2}%, var(--pwt-color-surface) {err_end:.2}% 100%);"
    )
}

fn run_mode_summary_columns() -> Rc<Vec<DataTableHeader<RunModeSummaryRow>>> {
    Rc::new(vec![
        DataTableColumn::new("")
            .flex(1)
            .get_property_owned(|item: &RunModeSummaryRow| item.mode.clone())
            .into(),
        DataTableColumn::new("")
            .width("110px")
            .render(|item: &RunModeSummaryRow| {
                Row::new()
                    .class(pwt::css::AlignItems::Center)
                    .gap(1)
                    .with_child(Fa::new("times-circle").class("pwt-color-error"))
                    .with_child(item.error_count.to_string())
                    .into()
            })
            .into(),
        DataTableColumn::new("")
            .width("110px")
            .render(|item: &RunModeSummaryRow| {
                Row::new()
                    .class(pwt::css::AlignItems::Center)
                    .gap(1)
                    .with_child(Fa::new("check").class("pwt-color-success"))
                    .with_child(item.ok_count.to_string())
                    .into()
            })
            .into(),
    ])
}

fn render_run_mode_summary_panel(runs: &[OffsiteReplicationRun]) -> Html {
    if runs.is_empty() {
        return html! {
            <div style="padding: 10px 12px; opacity: 0.8;">{tr!("No runs available yet.")}</div>
        };
    }

    let mut grouped: BTreeMap<String, RunModeSummaryRow> = BTreeMap::new();
    for run in runs {
        let mode = run
            .transfer_mode
            .clone()
            .unwrap_or_else(|| tr!("unknown").to_lowercase());
        let entry = grouped
            .entry(mode.clone())
            .or_insert_with(|| RunModeSummaryRow {
                mode,
                ok_count: 0,
                error_count: 0,
            });

        if run.success {
            entry.ok_count += 1;
        } else {
            entry.error_count += 1;
        }
    }

    let mut rows: Vec<RunModeSummaryRow> = grouped.into_values().collect();
    rows.sort_by(|a, b| {
        b.error_count
            .cmp(&a.error_count)
            .then_with(|| b.ok_count.cmp(&a.ok_count))
            .then_with(|| a.mode.cmp(&b.mode))
    });

    let store: Store<RunModeSummaryRow> = Store::new();
    store.set_data(rows);

    DataTable::new(run_mode_summary_columns(), store)
        .striped(false)
        .borderless(true)
        .hover(true)
        .virtual_scroll(false)
        .show_header(false)
        .into()
}

fn render_recovery_snapshot_state_tiles(points: &[OffsiteRecoveryPoint]) -> Html {
    let full = points
        .iter()
        .filter(|point| point.transfer_mode.as_deref() == Some("full"))
        .count();
    let incremental = points
        .iter()
        .filter(|point| point.transfer_mode.as_deref() == Some("incremental"))
        .count();
    let unknown = points
        .iter()
        .filter(|point| {
            !matches!(
                point.transfer_mode.as_deref(),
                Some("full") | Some("incremental")
            )
        })
        .count();
    let all = points.len();

    let tiles = vec![
        summary_list_tile(
            "check",
            Some("pwt-color-success"),
            tr!("Recoverable Points"),
            all,
        ),
        summary_list_tile("clone", None, tr!("Full Transfers"), full),
        summary_list_tile(
            "exchange",
            Some("pwt-color-warning"),
            tr!("Incremental Transfers"),
            incremental,
        ),
        summary_list_tile("question-circle", None, tr!("Unknown Mode"), unknown),
        summary_list_tile("th", None, tr!("Total Points"), all),
    ];

    List::new(tiles.len() as u64, move |idx: u64| {
        tiles[idx as usize].clone()
    })
    .padding(2)
    .class(pwt::css::Flex::Fill)
    .grid_template_columns("auto auto 1fr auto")
    .into()
}

fn snapshot_tail(snapshot: &str) -> String {
    snapshot.rsplit('@').next().unwrap_or(snapshot).to_string()
}

fn failover_lifecycle_text(state: OffsiteFailoverLifecycle) -> String {
    match state {
        OffsiteFailoverLifecycle::Active => tr!("Active"),
        OffsiteFailoverLifecycle::Returned => tr!("Returned"),
        OffsiteFailoverLifecycle::Abandoned => tr!("Abandoned"),
        OffsiteFailoverLifecycle::Superseded => tr!("Superseded"),
    }
}

fn guest_state_text(state: OffsiteGuestState) -> String {
    match state {
        OffsiteGuestState::Unknown => tr!("Unknown"),
        OffsiteGuestState::Running => tr!("Running"),
        OffsiteGuestState::Stopped => tr!("Stopped"),
        OffsiteGuestState::Missing => tr!("Missing"),
    }
}

fn failback_lineage_text(state: OffsiteFailbackLineage) -> String {
    match state {
        OffsiteFailbackLineage::Unchecked => tr!("Unchecked"),
        OffsiteFailbackLineage::Incremental => tr!("Incremental"),
        OffsiteFailbackLineage::FullRequired => tr!("Full required"),
        OffsiteFailbackLineage::Diverged => tr!("Diverged"),
    }
}

fn latest_runs_for_graph(
    runs: &[OffsiteReplicationRun],
    limit: usize,
) -> Vec<&OffsiteReplicationRun> {
    let mut selected: Vec<&OffsiteReplicationRun> = runs.iter().take(limit).collect();
    selected.reverse();
    selected
}

struct HistoryGraphSeries {
    time: Rc<Vec<i64>>,
    duration: Rc<Series>,
    transfer: Rc<Series>,
    success: Rc<Series>,
    failure: Rc<Series>,
}

fn history_graph_series(runs: &[OffsiteReplicationRun], limit: usize) -> HistoryGraphSeries {
    let points = latest_runs_for_graph(runs, limit);

    let mut time = Vec::with_capacity(points.len());
    let mut duration = Vec::with_capacity(points.len());
    let mut transfer = Vec::with_capacity(points.len());
    let mut success = Vec::with_capacity(points.len());
    let mut failure = Vec::with_capacity(points.len());

    for run in points {
        time.push(run.end_time);
        duration.push(run.duration.max(0) as f64);
        transfer.push(
            run.transferred_bytes
                .or(run.estimated_bytes)
                .map(|value| value as f64)
                .unwrap_or(f64::NAN),
        );
        if run.success {
            success.push(1.0);
            failure.push(f64::NAN);
        } else {
            success.push(f64::NAN);
            failure.push(0.0);
        }
    }

    HistoryGraphSeries {
        time: Rc::new(time),
        duration: Rc::new(Series::new(tr!("Duration"), duration)),
        transfer: Rc::new(Series::new(tr!("Transferred"), transfer)),
        success: Rc::new(Series::new(tr!("OK"), success)),
        failure: Rc::new(Series::new(tr!("Error"), failure)),
    }
}

fn graph_card(widget: impl Into<Html>) -> Html {
    Container::new()
        .padding(2)
        .style("min-width", "0")
        .with_child(widget)
        .into()
}

fn render_duration_trend_graph(graphs: &HistoryGraphSeries) -> Html {
    graph_card(
        RRDGraph::new(graphs.time.clone())
            .title(tr!("Duration Trend"))
            .render_value(|v: &f64| {
                if v.is_finite() {
                    format_duration_human(*v)
                } else {
                    "-".to_string()
                }
            })
            .serie0(Some(graphs.duration.clone())),
    )
}

fn render_transfer_trend_graph(graphs: &HistoryGraphSeries) -> Html {
    graph_card(
        RRDGraph::new(graphs.time.clone())
            .title(tr!("Transfer Trend"))
            .binary(true)
            .render_value(|v: &f64| {
                if v.is_finite() {
                    HumanByte::from(*v as u64).to_string()
                } else {
                    "-".to_string()
                }
            })
            .serie0(Some(graphs.transfer.clone())),
    )
}

fn render_result_trend_graph(
    graphs: &HistoryGraphSeries,
    success_count: usize,
    total_count: usize,
) -> Html {
    graph_card(
        RRDGraph::new(graphs.time.clone())
            .title(format!(
                "{} ({success_count}/{total_count} ok)",
                tr!("Result Trend")
            ))
            .serie0(Some(graphs.success.clone()))
            .serie1(Some(graphs.failure.clone())),
    )
}
