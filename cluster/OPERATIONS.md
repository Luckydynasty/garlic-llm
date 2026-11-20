# Durable queue: offline milestone

This development version uses schema 2. It has been tested with mock transports on Windows PowerShell 5.1, including a separate process holding an endpoint lock. Physical console/API qualification is still required before enrollment. No service or listener is installed.

Use one **absolute local StateDirectory on the same NTFS volume**, shared by every coordinator process and Windows session. Do not use different directories for the same consoles, cloud-synced folders, network shares, or multiple controller PCs. The locks coordinate only processes sharing that store. Lock files remain as inert files after release; deleting them while a worker is active defeats ownership. Endpoint addresses must be canonical static addresses on the isolated segment.

```powershell
Import-Module .\Coordinator.psm1 -Force
$state = 'C:\path\to\private-cluster-state'
# nodes.local.json is ignored by Git; enroll only after authentication/API verification.
$job = Submit-ClusterJob -Config .\nodes.local.json -NodeId ps4-pro `
    -Prompt 'Explain proportional control.' -StateDirectory $state
Get-ClusterJobs -StateDirectory $state
# Runs at most one oldest queued job, synchronously. Invoke again to drain more.
Invoke-ClusterWorker -Config .\nodes.local.json -NodeId ps4-pro -StateDirectory $state
Get-ClusterHealth -Config .\nodes.local.json -StateDirectory $state
```

Different nodes may be processed by separate worker processes using the same store. The store lock is short lived; the endpoint lock spans the request. A locked store/endpoint returns an error instead of waiting indefinitely. A final-save lock conflict leaves the durable running record intact; treat it as unresolved and reconcile manually. No background queue draining, automatic load balancing or failover exists yet.

Transitions: queued → running → succeeded/unknown; queued → cancelled; running/unknown → resolved after explicit operator review. No transition automatically resends work. Each state change records a timestamp and explanation. `succeeded` means a syntactically valid completion with a string content and recognized finish reason was received. It does not establish answer quality; `length` can mean token limit or controller stop.

To cancel **unsent queued work**:

```powershell
Resolve-ClusterJob -StateDirectory $state -JobId $job.id -Action CancelQueued `
    -Note 'This prompt is no longer needed.'
```

After a worker crash or unknown delivery, inspect the PS4, stop generation there if necessary, confirm it is idle, and then record reconciliation:

```powershell
Resolve-ClusterJob -StateDirectory $state -JobId '<32-character job ID>' `
    -Action ConfirmConsoleIdle -Note 'Inspected console and confirmed no request is running.'
```

This does not label the original answer successful or resend it. It unlocks subsequent queued work. The command refuses resolution while a cooperating process owns the endpoint. A node rename or endpoint change cannot bypass an unresolved record in the same store. Queued requests retain their original destination and refuse silent relocation. There is no remote Stop endpoint in this coordinator.

Writes use a flushed temporary file and atomic replacement; an interrupted write preserves either the previous complete record or the new record. This reduces partial-record risk but is not a substitute for backups or a UPS. Corrupt/unsupported job records block dispatch instead of being skipped. Restore a known backup and inspect the console before resuming. Do not run workers against copies of the same live store.

**Migration:** the earlier prototype wrote unversioned records. These fail closed in the new reader. Before adopting this version, inspect every prior running/unknown job against its console and archive the old store as read-only. Start schema 2 in the designated canonical store only after every old request is accounted for. There is intentionally no destructive automatic migration.

API errors/timeouts/malformed responses conservatively become unknown. Missing local keys leave queued jobs unsent. Health results do not prove engine readiness and avoid probing a locally owned endpoint. HTTP auth remains plaintext over the dedicated isolated cable/switch; no network exposure is added. Response reads are bounded to roughly 2 million decoded characters, with socket timeouts and elapsed-time checks between chunks; this is not a strict end-to-end wall-clock deadline. The full-history/context and SSE streaming milestones remain pending.

`Invoke-ClusterJob` is a convenience wrapper returning both `SubmittedJob` and `ProcessedJob`: the worker processes the oldest queued request, which can differ from the just-submitted one. New integrations should use the explicit queue commands above.

Records contain private prompts and full answers. Keep StateDirectory outside the repository or under ignored `cluster/state/`; restrict Windows account access, and back up deliberately. No keys or exception text containing request credentials are stored in job notes.
