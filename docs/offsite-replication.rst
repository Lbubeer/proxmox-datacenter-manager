Off-site Replication
--------------------

Off-site replication provides scheduled ZFS-based guest replication between
independent Proxmox VE remotes managed by Proxmox Datacenter Manager.

The feature is intended for manual recovery and disaster-recovery drills. It is
not high-availability orchestration.


Reviewer Deployment
~~~~~~~~~~~~~~~~~~~

Minimal setup for reviewing the feature:

* one Proxmox Datacenter Manager host
* two Proxmox VE remotes (source and target)
* one test QEMU VM on source storage that supports snapshot replication

Prerequisites before creating jobs:

* root SSH access must be enabled between the involved nodes (``source -> target``
  for pve-zsync, and ``PDM -> source`` plus ``PDM -> target`` for run-now and
  failover operations)
* the SSH private key configured in the off-site job must exist on the PDM host
  and be readable by the PDM privileged service user

To evaluate qemu-guest-agent freeze/thaw behavior, install and start
``qemu-guest-agent`` inside the test VM.

In the web interface:

1. Add both Proxmox VE remotes.
2. Open ``Configuration -> Off-site Replication``.
3. Create one job with:
   * source remote/node + guest
   * target remote/node + dataset
   * schedule
   * ``Max Snapshots`` and ``History Limit``
4. Optionally use the SSH setup assistant in the job editor to prepare key
   connectivity and remote authorization.


Smoke Test
~~~~~~~~~~

Recommended basic review flow:

1. Trigger ``Run Now`` for the created job.
2. Confirm a task UPID is returned and finishes successfully.
3. Open ``Metrics / History`` and verify:
   * a new run entry is recorded
   * parsed transfer fields are populated
   * history retention respects ``History Limit``
4. Open ``Failover / Restore``, select a recovery snapshot and run
   ``Promote`` (or ``Promote + Start``).
5. Confirm a recovery VM is created on the target node.
   The job is suspended while an active promoted guest exists, preventing the
   scheduler from writing misleading failures against an unavailable source.
6. Select the promoted guest record, run ``Failback Precheck``, then choose
   either ``Replace Original`` or ``Restore As New`` before returning it to the
   source node. The failback task snapshots the promoted guest's current disk
   state itself; it does not reuse the recovery point selected for promotion.

For promotions created from this feature version, recovery disks are ZFS clones
of the selected recovery snapshots. This preserves the ZFS lineage required for
an incremental failback to the original guest. Older recovery VMs, or recovery
VMs whose clone lineage is no longer available, are shown by the precheck as
requiring an explicit full transfer. The precheck compares ZFS snapshot GUIDs,
not only snapshot names. A full transfer can be restored under a new VMID
without replacing an existing source guest.

Promoted guest records retain their audit lifecycle and also show reconciled
source VM, target VM, and ZFS lineage state. If a recovery VM is changed or
removed outside PDM, refresh the panel to see its current state. Use
``Archive / Abandon`` to close a stale workflow explicitly; records are not
silently removed. Archiving does not delete a target VM: remove any retained
promoted guest before using ``Resume Replication``.

Encrypted ZFS stream smoke path:

1. Place a test VM disk on an encrypted ZFS dataset on the source.
2. Create a job with ``ZFS Stream Mode`` set to ``auto`` (or ``raw``).
3. Trigger ``Run Now`` and verify the run log contains
   ``zfs-stream configured=... effective=raw``.
4. Run failover from ``Failover / Restore`` and verify the recovery VM can be
   created from the encrypted recovery snapshot chain.


Validation Notes
~~~~~~~~~~~~~~~~

* The scheduler executes due jobs from the privileged daemon.
* ``qga-fsfreeze=requested`` can still show "freeze skipped" if the guest agent
  is unavailable in the VM.
* ``zfs-stream=auto`` selects raw stream mode for encrypted source datasets.
* Recovery-point retention is bound by ``Max Snapshots``. Run history retention
  is controlled separately via ``History Limit``. Recoverable points are stored
  in a dedicated catalog, so reducing or exhausting run history does not hide
  snapshots that remain usable on the target.
* Failback stops the promoted guest before taking its return snapshot. Target
  cleanup is optional and is performed only after the source-side guest has
  been registered successfully. When the promoted target guest is retained,
  replication stays suspended until that guest is removed so its ZFS clone
  cannot pin snapshots needed by retention cleanup.
* Full fallback has no common source/target lineage. With target cleanup
  enabled, PDM therefore resets only the affected replication datasets after
  source registration so the next run can establish a new full baseline. The
  reset refuses datasets containing snapshots outside the current job.
* Replacement failback receives all returned disks into source-side staging
  datasets before removing the old source VM. A failed transfer leaves the old
  source untouched; a failed final registration leaves the returned datasets
  available for diagnosis and retry. Incremental failback seeds each staging
  dataset from the common source snapshot before applying the promoted guest's
  delta; staging is independent rather than a clone so ZFS can receive it. The
  promoted target clone is temporarily promoted while producing a normal
  incremental stream, then the original target lineage is restored before
  cleanup.
* Promoted VMs might require manual NIC/network adjustments depending on source
  and target network topology.
