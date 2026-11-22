# Durable queue, streaming and conversations

## Endpoint identity hardening

Node addresses must exactly match the canonical dotted-decimal spelling returned by .NET, and port 8080 must be a JSON integer. Alternate address spellings such as `192.168.77.003` and string ports such as `"08080"` are rejected before submission or dispatch. Some network parsers treat these as the same endpoint, while the coordinator's lock filenames and job records use their configured spelling. Rejecting aliases prevents a renamed node from acquiring a second lock identity or bypassing unresolved-delivery protection. The dedicated-subnet restriction remains in place. `Test-Coordinator.ps1` covers aliases, valid canonical endpoints, and attempted alias dispatch while an unresolved job exists.

## Network simulation milestone

`Test-Network.ps1` starts two synthetic Node.js HTTP servers on **127.0.0.1 only**, using ephemeral ports and a fixture key. It stops the server process in its cleanup block. Node.js must be on PATH to run this test; the production coordinator still needs only Windows PowerShell/.NET. No administrator HTTP URL reservation or firewall opening is needed. The test module instance bypasses enrollment solely for fixture queue tests; shipped `Read-ClusterNodes` still rejects loopback and non-8080 production endpoints. It does not connect to PS4s.

Verified over actual HTTP sockets: chunked SSE with bytes split through multi-byte Unicode characters, incremental callbacks before completion, two server requests active simultaneously with distinct answers, missing completion after a socket drop, invalid UTF-8, 401, 503, redirects (not followed), header stalls, body stalls, and continuous heartbeat trickle. Queue integration verifies a dropped stream preserves partial progress, becomes unknown, blocks replay, and permits an explicitly reconciled new job to succeed.

Transport hardening: a small compiled .NET helper (`HttpDeadline.cs`, loaded on module import) aborts the HTTP request at its overall timeout, including request upload, response headers and blocked body reads. Per-read socket timeouts remain as a second bound. This fixes the earlier between-chunk elapsed check, which could not independently interrupt a blocked read or perpetual trickle. Timer scheduling is subject to OS scheduling; test tolerance is six seconds for a one-second timeout. The worker exposes `-TimeoutSeconds` (1–600, default 180). Whole responses require JSON content type and HTTP 200; streams require SSE content type. Redirects and error responses are rejected, and response resources are disposed. No retry semantics change: failures after dispatch remain conservatively unknown.

Run all three suites in order: `Test-Coordinator.ps1`, `Test-Streaming.ps1`, `Test-Network.ps1`. GitHub Windows CI runs all three. Loopback does not reproduce PS4-specific socket implementations, performance, restarts or LAN loss; physical API qualification remains required.

## Streaming and conversation milestone

```powershell
$job = Submit-ClusterJob -Config .\nodes.local.json -NodeId ps4-pro `
    -Prompt 'Explain proportional control.' -StateDirectory $state `
    -ConversationId controls-demo -Stream
# Run in a separate worker process, with the same absolute $state:
Invoke-ClusterWorker -Config .\nodes.local.json -NodeId ps4-pro -StateDirectory $state
# While it runs, poll from the controller:
Get-ClusterProgress -StateDirectory $state -JobId $job.id
```

Streaming uses Garlic's documented SSE format, validates choice 0, accumulates Unicode text, and requires both a recognized finish reason and `[DONE]`. Premature EOF, invalid JSON, oversized lines/streams, or stream errors leave the job unknown. Progress files (`<job>.progress`) are atomically replaced; their text is partial until a completed job is saved. Readers can briefly see a sharing conflict while a replacement occurs; retry a read, never retry generation. Check the authoritative job record too: a worker crash can leave stale progress. Stream metadata such as usage counts is not reconstructed. No remote cancellation endpoint is added.

Conversation IDs are local, case-insensitive labels (letters, numbers, hyphens). A subsequent turn sends every prior successful user/assistant pair plus the new prompt in one request. Completed turns can be routed explicitly to a different enrolled node. Different conversations never share messages. This is explicit full-history resubmission because Garlic resets console chat per API request. A conversation permits only one queued/running turn. Unknown/running/resolved turns block continuation; after manual reconciliation use a new ID to avoid silently dropping an uncertain exchange. Cancelled unsent turns do not enter context. Conversations have no system-prompt editor, branching UI or summary compaction yet.

The default context budget is **4096 tokens**, conservatively bounded by UTF-8 content bytes + 32 template tokens per message + 64 reserved tokens + requested output tokens. This is an intentionally restrictive upper-bound estimate for the current byte-based tokenizer path, not exact token counting. Set optional integer `contextTokens` (1024–16384) on a node only after verifying its actual context capacity. Validation occurs before queueing and again before dispatch against the current node budget. Oversized requests are refused with no history truncation. Sessions with long answers will reach this conservative limit earlier than exact tokenization would. A lower-memory or different model requires deliberate configuration and qualification.

Jobs remain schema 2 with optional `messages`, `conversation`, and `streaming` fields; previously queued schema-2 jobs work as one-turn, non-streamed requests, subject to the new budget checks. Progress and job files contain private text and should be backed up/protected together. Streaming writes a complete accumulated progress snapshot on each content event, so high-throughput use needs future throttling before large-scale deployment.

Offline validation uses two separate PowerShell worker processes, synthetic SSE readers, a barrier requiring both mock transports to be active simultaneously, and checks per-node Unicode results/progress. Tests also cover missing DONE, oversized SSE lines, history carryover across nodes, pending-turn isolation, and pre-dispatch budget refusal. This does not qualify real sockets, physical consoles, or their performance; a network-level mock server and PS4 API qualification remain future integration gates.

The guidance below describes the underlying queue and recovery behavior; the earlier statement that streaming was pending is superseded by this milestone. Exact tokenizer/context optimization and API hardware validation remain pending.

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

API errors/timeouts/malformed responses conservatively become unknown. Missing local keys leave queued jobs unsent. Health results do not prove engine readiness and avoid probing a locally owned endpoint. HTTP auth remains plaintext over the dedicated isolated cable/switch; no network exposure is added. Response reads are bounded to roughly 2 million decoded characters and request-wide timeout abortion as described above. Physical context/tokenizer qualification remains pending; full-history resubmission and SSE are implemented in the current prototype.

`Invoke-ClusterJob` is a convenience wrapper returning both `SubmittedJob` and `ProcessedJob`: the worker processes the oldest queued request, which can differ from the just-submitted one. New integrations should use the explicit queue commands above.

Records contain private prompts and full answers. Keep StateDirectory outside the repository or under ignored `cluster/state/`; restrict Windows account access, and back up deliberately. No keys or exception text containing request credentials are stored in job notes.
