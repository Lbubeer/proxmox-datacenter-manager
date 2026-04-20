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
  is controlled separately via ``History Limit``.
* Promoted VMs might require manual NIC/network adjustments depending on source
  and target network topology.
