# Moving the LFO control plane into Datadog-side crawlers

Assumes per-customer Azure app registrations are available for authentication, and that crawler infrastructure on the Datadog side is provided separately. Scope: changes to LFO itself.

## Authentication
- Replace `DefaultAzureCredential` (used today because the function apps run with managed identity inside the customer's resource group) with a per-customer credential built from the app-registration's tenant id + client id + secret/cert. Concretely: `tasks/common.py:create_credential()` becomes a function that takes a customer/tenant context and returns a `ClientSecretCredential` (or federated `WorkloadIdentityCredential` if Datadog is set up as a trusted issuer).
- The credential needs to be plumbed through every Azure SDK client in `tasks/client/*` (resource client, log-forwarder client, etc.), so per-call context replaces "ambient identity from process env."
- Role assignments on customer subscriptions move from the function app's MI to the app registration. The bits of `uninstall.py` that look at role assignments tagged `ddlfo*` need to follow.

## State storage
- All control-plane caches currently in lfostorage's `control-plane-cache` container — `assignments.json`, `resources.json`, `settings_event.json`, the deployer's private `manifest.json` — move to Datadog-side storage (whatever the crawler platform provides: blob, KV, DB row, etc.).
- `cache/common.py:read_cache/write_cache` becomes the single seam: re-implement against the Datadog-side store and the rest of `cache/*.py` keeps its current shape (schemas + serializers don't need to change).
- Per-customer namespacing has to be added — today the cache is implicitly scoped because each customer has its own lfostorage. In the multi-tenant crawler, every read/write needs a customer/deployment id in the key.

## Scheduling and task lifecycle
- Timer triggers in `control_plane/config/*/function.json` go away. The crawler platform schedules the three tasks per customer deployment (every ~5 min, like today). `__main__.py` / `task_main` becomes a per-invocation entrypoint rather than a long-lived process.
- The deployer task is eliminated entirely. Its sole job is shipping the resources/scaling/diagnostic-settings function-app zips into the customer's function apps; after this change those tasks run inside Datadog. With it go: `tasks/deployer_task.py`, `cache/manifest_cache.py`, the `ddazurelfo` public manifest, `scripts/publish.py`, and the build pipeline that produces `*_task.zip`.

## Configuration
- Today config is read from process env (`MONITORED_SUBSCRIPTIONS`, `RESOURCE_TAG_FILTERS`, `SCALING_PERCENTAGE`, `DD_API_KEY`, `DD_SITE`, `PII_SCRUBBER_RULES`, `CONTROL_PLANE_ID`, `CONTROL_PLANE_REGION`, `AZURE_AUTHORITY`, etc.) — see `cache/env.py`.
- Source of truth becomes the customer's LFO deployment record on the Datadog side. `cache/env.py` becomes a thin adapter that pulls from that record instead of `os.environ`. `CONTROL_PLANE_ID` and region come from the same record (replacing the `lfostorage{id}` name-derivation in `run_task.sh` and `uninstall.py`).

## Customer-side footprint
- The only customer-side resources LFO still creates/manages are the data plane: per-forwarder Container App Jobs, their `ddlogstorage*` storage accounts (still used for forwarder→scaling-task metric blobs and as the forwarders' `AzureWebJobsStorage`), the managed environments, and the diagnostic settings on monitored resources. `log_forwarder_client.py` keeps its responsibilities — it just executes them remotely via the app-registration credential.
- Customer onboarding becomes: create the app registration, grant it the required roles on monitored subscriptions, hand the credentials to Datadog. No control-plane resource group, no function apps, no app service plan, no lfostorage.

## Uninstall / discovery
- `uninstall.py`'s "find by `lfostorage*` name prefix" approach no longer works. Switch to tag-based discovery (everything LFO creates is already tagged) or, better, to a Datadog-side inventory of what was provisioned for each deployment. Same for the role-assignment cleanup, which was keyed on description `ddlfo{id}`.

## Things that simply disappear
- The forwarder self-loop filter in `forwarder/internal/logs/client.go:61-65` (AZINTS-4446) and the `is_control_plane_storage_account` exclusion in `tasks/client/resource_client.py` exist solely because lfostorage was a customer-side resource the data plane would otherwise pick up. With lfostorage gone, both can be deleted outright.
- `scripts/run_task.sh` becomes obsolete in its current form — there's no in-customer storage account to point at. If still useful for local dev, it has to be reworked to point at a local/dev Datadog-side store and accept `CONTROL_PLANE_ID` as an explicit argument.

## Telemetry
- Today the tasks ship telemetry (logs/metrics) by calling the Datadog API with the customer's DD API key (`tasks/telemetry.py`, `tasks/task.py:164`). When the crawler runs inside Datadog, decide whether control-plane telemetry stays on the customer's DD org or moves to internal Datadog observability.

## Do we still need lfostorage?

No. Every remaining reason to keep it disappears under this model:

| Current use | Replacement |
|---|---|
| `AzureWebJobsStorage` for the four function apps | No function apps |
| `control-plane-cache` blob container (assignments / resources / diag-settings event / private manifest) | Datadog-side state store |
| Identity anchor for `CONTROL_PLANE_ID` (run_task.sh, uninstall.py) | Datadog deployment record |
| Public-manifest deployer plumbing | Deployer task removed |
| Self-monitoring guards in forwarder + resource_client | Deleted — no in-customer LFO storage to filter out |

The customer-side footprint shrinks to the data plane (forwarder jobs + their per-forwarder `ddlogstorage*` accounts) plus the diagnostic settings the crawlers create on monitored resources.
