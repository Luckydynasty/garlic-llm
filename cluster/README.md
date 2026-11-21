# Garlic cluster pre-development

**HTTP qualification update:** Two loopback simulated Garlic servers now exercise real chunked HTTP streaming, fragmented Unicode, simultaneous requests, stalls/trickle deadlines, disconnect recovery and authentication/error/redirect rejection. The request-wide abort deadline fixes the earlier reliance on per-read timeouts. See [OPERATIONS.md](OPERATIONS.md). Requires Node.js for network tests only; no console was contacted or updated.

**Streaming/conversation update:** Optional `-Stream` now validates SSE completion and saves per-job progress. `-ConversationId` resubmits completed turns with conservative context admission checks. Two separate simulated node workers have been exercised concurrently, including Unicode results and conversation isolation. See [OPERATIONS.md](OPERATIONS.md) for usage and limits. Real console/socket qualification is still pending.

**Hardening update:** The branch is published at [Luckydynasty/garlic-llm](https://github.com/Luckydynasty/garlic-llm/tree/cluster/predevelopment/cluster). The coordinator now provides a durable FIFO queue, explicit reconciliation with audit events, endpoint file locks across Windows sessions, and atomic record replacement. Read [OPERATIONS.md](OPERATIONS.md) for the current commands, schema-2 migration and limitations. Earlier prototype descriptions below are historical where they differ (especially mutexes, health signature and Invoke-ClusterJob's result). Console/API qualification is still pending.

Local development branch `cluster/predevelopment`, based on upstream commit `22bb871a1ad3583624b910068e3d5822d9ded99d`, inspected November 20, 2026. This is a coordinator prototype, not a production cluster or a published GitHub fork. No console code, firmware, installed Windows client, or network settings were changed.

## New upstream evidence

The [official download page](https://garlicllm.dev/download/) now offers **1.20 experimental**, including API support and native Pro mode. Earlier project notes saying the API was only in source are superseded for that experimental package. Version 1.19 remains the stable package. Published 1.20 experimental SHA-256: `c0e8d289216b277414994b91fc71eefb23fd7ec84ecf1ce18d5dfe3048d82c2f`. The package was not downloaded, built or tested here. Actual compatibility and performance on the incoming CUH-7215 remain unverified. The Pro's firmware must be inspected first.

## Source audit

All source links below are pinned to the inspected commit:

| Finding | Evidence | Consequence |
|---|---|---|
| Complete local model and state per engine | [engine.c](https://github.com/leonardo-alegria/garlic-llm/blob/22bb871a1ad3583624b910068e3d5822d9ded99d/chat/engine.c), [api_run.inc](https://github.com/leonardo-alegria/garlic-llm/blob/22bb871a1ad3583624b910068e3d5822d9ded99d/chat/api_run.inc) | Start with independent workers; no documented model sharding |
| `api_main` accepts and calls `handle` sequentially; `serve_chat` waits until `job->done` | [api.c](https://github.com/leonardo-alegria/garlic-llm/blob/22bb871a1ad3583624b910068e3d5822d9ded99d/chat/api.c) | Health can wait behind generation. Do not mark an active worker dead or launch duplicate work based on a health timeout |
| `/health` returns static `status:ok` | Same file, `handle` | Reachability is not engine readiness, capacity or job status |
| Listener binds any console address on port 8080; wildcard CORS; optional bearer key read once during initial setup | Same file, `api_listen`, `authorized`, `api_setup` | Dedicated wired segment required. Provision key before first enable; restart app after changing it. Bearer HTTP is unencrypted |
| API resets conversation, disables reasoning and reconstructs prompt from request messages | `api_run.inc`, `api_job`, `api_prompt` | PC owns session history. Do not mix interactive controller chat with API worker mode. Think UI setting does not enable API reasoning |
| Prompt and requested answer must fit engine context; `API_IDS` is 16384 with reserved tail | `api_run.inc` | Enforce context budgets rather than concatenate history without bounds |
| `finish_reason:length` covers token limit and Circle interruption | `api_run.inc` | Cannot infer cancellation from finish reason alone |
| Streaming notices disconnected clients during writes; whole-response mode cannot reliably cancel early on disconnect | `api_run.inc` | Closing the Windows request is not a verified cancel protocol |
| Generation parser permits temperature 0–2, max tokens 1–16384 | [api_proto.c](https://github.com/leonardo-alegria/garlic-llm/blob/22bb871a1ad3583624b910068e3d5822d9ded99d/chat/api_proto.c) | Prototype deliberately uses narrower 0–1.5 and 1–2048 limits |

## First implementation

`Coordinator.psm1` runs in Windows PowerShell 5.1 with no extra dependency. It opens no server, uses no proxy or redirect, and connects only to allowed hosts on the existing isolated `192.168.77.0/24` segment at port 8080. It supports explicit node selection, read-only health checks, bounded whole-response requests and persistent job records. It will not retry or reroute a failed/ambiguous request. It uses per-node Windows-session mutexes and blocks nodes with unresolved running/unknown job records. All coordinator invocations must use one Windows account/session and the same StateDirectory. Other computers or sessions are not coordinated by those locks.

Example config includes the current PS4 at `.2` and **proposed**, unassigned Pro address `.3`. Both are disabled and API-unverified. They cannot receive requests until deliberately enrolled. No changes were made to your actual addresses.

```powershell
Import-Module .\Coordinator.psm1 -Force
Get-ClusterHealth -Config .\nodes.example.json
```

To enroll a proven API node later: make an ignored `nodes.local.json`, confirm its actual address, provision `/data/garlic_api_key.txt` on the console, restart Garlic, turn on API on the isolated segment, set the corresponding process environment variable, and only then set `enabled` and `apiVerified` true. Do not place actual keys in configuration, Git or command history. A successful HTTP request does not prove that the server enforces the key: verify a wrong key returns 401 before enrollment.

```powershell
# After deliberate node enrollment; this clears that PS4's visible chat.
Invoke-ClusterJob -Config .\nodes.local.json -NodeId ps4-pro `
  -Prompt 'Explain proportional control briefly.' -StateDirectory .\state
```

Records contain private prompt and response text. Keep them on the local PC and out of source control. If a request ends as `unknown`, or a crashed process leaves `running`, stop using that worker until its console state has been inspected. Archive the record outside StateDirectory only after manual reconciliation. This is a development recovery procedure; a proper audited reconcile command is a next milestone. `succeeded` means a valid response was received, including responses truncated by a token limit; inspect its finish reason. No durable background queue, automatic scheduling, streaming, remote cancel or GUI integration is claimed yet.

## Roadmap and gates

1. **Hardware intake:** record full Pro model/firmware, establish supported GoldHEN route, test 1.19 independently; only then evaluate official 1.20 experimental on the test node. Preserve reference PS4 and rollback packages. No promised speedup until measured.
2. **Protocol qualification:** wrong-key refusal, models/health, short/full/streamed Unicode responses, long context refusal, busy behavior, disconnect, restart and Circle stop. Compare stock experimental package against pinned source behavior. Existing upstream host C tests cover protocol/generation helpers; they do not validate PS4 sockets or GPU behavior.
3. **Coordinator hardening:** durable queue with job states, one service owner across sessions, immutable node identity/endpoint ownership, explicit uncertainty reconciliation, structured error classes, streaming progress and per-session history/context budgets. Only enable failover before confirmed delivery; never blindly replay an unknown job.
4. **Two-node validation:** isolated switch, `.1` controller and distinct verified worker IPs; no forwarding/bridge/gateway to household LAN. Separate conversations and queues per node; fixed benchmark prompts, latency distributions, jobs/hour and sustained power/thermal observations. Failure of one worker must leave the other usable.
5. **Windows interface integration:** selectable node, per-node state, full API text rather than log previews, retained FTP maintenance mode clearly separated. No automatic upgrade of the existing client.
6. **Engine fork only if qualification reveals a blocker:** propose capability/build identity and concurrent bounded health handling first; then request IDs/deduplication and job status/cancel. Do not invent endpoints in the PC client before a console patch exists and is proven. API Think support needs its own reviewed engine change. Model sharding remains a separate feasibility project.

Acceptance: two independent jobs execute concurrently on two physical nodes; prompt/result attribution is correct; no silent retries; restart ambiguity is visible; no API/FTP reachability from household LAN; recover one node independently. Use separate processes with one shared StateDirectory for prototype concurrency experiments.

## Fork and build handoff

The checkout retains upstream history and GPLv3 LICENSE/CREDITS. `upstream` points to the original repository; there is no push remote. A GitHub fork can be published after authenticating your GitHub connection. Do not push to upstream or include local records/keys/models. Review the diff, create your GitHub fork, add it as `origin`, then push `cluster/predevelopment`. No PR is opened automatically.

For a PS4 build, follow pinned upstream `README.md` and `build_chat.py`: Python plus NumPy/Pillow, OpenOrbis, Zig, .NET packaging tools and model assets are required. These are not installed by this prototype. No PS4 PKG was built, deployed or runtime-verified. Preserve license notices and provide corresponding source for distributed modified GPL binaries; models retain their separate licenses.
