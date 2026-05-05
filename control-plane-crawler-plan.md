# Plan: port the LFO control plane to Datadog-side crawlers

Companion to `control-plane-crawler-migration.md`.

## Prototype scope

This is a prototype, not a production migration. To keep it tractable we are taking the following as given and explicitly out of scope:

1. **Clean break, new customers only.** No migration path for existing in-customer deployments; they continue running on the function-app control plane untouched.
2. **One integration per subscription.** Each monitored subscription has exactly one `AppRegistrationIntegration` associated with it — no fan-in, no multi-integration handling.
3. **Forwarder resource group already exists** in each monitored subscription. The crawler does not create or manage it; it just creates resources inside it.
4. **App-registration permissions are already granted.** The Phase 5 role-assignment list is treated as a hand-off requirement, not something the prototype provisions or verifies. The prototype assumes everything in that list is in place before crawlers run.
5. **The LFO resources crawler maintains its own resources cache** in `bcfg.KV`, ported one-to-one from `cache/resources_cache.py`. In production we may want to reuse the existing `azure_resource_discovery` `resource_management` cache (already in `bcfg.KV` per (org, subscription)) rather than collect resources twice — but reconciling the two cache shapes and filter rules is real work and not in prototype scope.
6. **No extra networking resources.** Managed environments are created with default networking; the prototype does not configure VNet integration, private endpoints, NSG rules, subnet delegation, or any other customer networking customizations. Customers requiring those configurations are out of scope.
7. **Reuse the metrics tag filters for log-forwarding filtering.** The prototype calls `integration.GetTagFilters(...)` on the existing metrics-side filters as a stand-in for log-forwarding filters. In production this needs its own config path: a customer may want metrics on a resource but not logs (or vice versa), and the LFO-specific config surface today (PII scrubber rules, scaling percentage, DD site, etc.) doesn't have a home on the integration record yet — those need a real schema and storage path on the Datadog side.

   In particular, the **DD API key the forwarders use** needs explicit customer choice. Today the function-app `DD_API_KEY` env var gets baked into each forwarder's secrets; the prototype will hardcode or take a single org-level value, but production must let a customer pick the key (which org/team, which permissions) for the forwarders independently of whatever credentials drive the integration itself. That means: a config field on the LFO config record that stores (or references the enclave-stored) API key the scaling crawler stamps into each forwarder's container-app-job secrets.
8. **The diagnostic-settings event cache (`lfo_settings_event`) stays as a state ledger in `bcfg.KV`.** It exists only to dedupe the "this resource already has the Azure max of 5 diagnostic settings" warning across crawl runs. In production we drop the cache entirely and query the Datadog Events API for an existing event keyed on the resource id, the same way `credentialexpirations.getExistingEvents` does today via `bcfg.EvpClient`. Source of truth becomes the events themselves rather than per-(org, subscription) state we maintain.

When productionizing, each of these gets revisited: existing-customer migration, multi-integration topologies, automated bootstrap of the resource group, a verified onboarding flow that grants the role assignments, reuse of the shared resource cache, proper networking support, a dedicated LFO config schema covering tag filters, customer-selected DD API key, and the rest of today's env-var settings, and replacing the diagnostic-settings event cache with Events API queries.

## Locked-in decisions

- **Clean break.** New customers only. Existing in-customer deployments stay on the function-app control plane and are out of scope.
- **Per-subscription invocation.** All three crawlers use `SubscriptionMiddleware`. The platform decides which (org, subscription) tuples to crawl — the control plane no longer discovers monitored subscriptions itself.
- **Port to Go in `dd-source`, register with the existing `apps/generic-crawler/`.** Three new crawlers under `apps/generic-crawler/internal/jobs/lfo/`. The Python `control_plane/` tree is deleted as part of the migration.
- **Package layout: one package, three files.** `internal/jobs/lfo/{resources_crawler.go, scaling_crawler.go, diagnostic_settings_crawler.go}`, matching the `credentialexpirations/` pattern.
- **State backend = `bcfg.KV`** (`crawlstate.KVCache`), keyed per (org, subscription) the way `resource_discovery` already does it.
- **Auth = `AppRegistrationIntegration.MakeCredentials(ctx, bcfg)`.** Framework handles secret fetch from the enclave, token caching, and cloud-environment selection.
- **Telemetry = statsd only.** No `bcfg.MetricsIntake` — that's for metrics submitted on the customer's behalf, which the control plane is not. Internal observability goes through statsd (and `cloudobservability.GetLogger` for logs, `crawlsummary.Increment` for crawl-summary counters as today's existing crawlers do).
- **Forwarder identity (`config_id`).** Mimic the current control plane: fresh random 12-char hex id per forwarder, minted at scale-up via the Go equivalent of `tasks/common.py:generate_unique_id`. Used for `ddlogstorage{config_id}` (24-char Azure limit, no headroom) and `dd-log-forwarder-{config_id}`. No derivation from tenant/client.
- **Forwarder image source.** Stays at `datadoghq.azurecr.io/forwarder:latest`; container-app-job spec references it directly. No deployer-equivalent needed.
- **Forwarder metrics path.** Unchanged — forwarders write `metrics_*.json` to their per-forwarder `ddlogstorage{config_id}` accounts; the scaling crawler reads them via `MakeCredentials` + Azure SDK.
- **Forwarder storage auth: account-key flow.** Mimic today (`log_forwarder_client.py:427-434`): the scaling crawler calls `storage_accounts.list_keys(...)` on each per-forwarder storage account, builds a connection string from the returned key, and reads metric blobs with that. No AAD blob auth in scope of the port.

## Two repos in play

- **`dd-source`** — all new Go code: the three LFO crawlers, registrations, any LFO-specific helpers under `apps/generic-crawler/internal/jobs/lfo/`.
- **`azure-log-forwarding-orchestration`** — net deletions: `control_plane/` (all of it), `scripts/publish.py`, `scripts/run_task.sh`, the Python build pipeline, the in-customer Bicep modules for the control plane, the lfostorage filter in `forwarder/internal/logs/client.go`. The forwarder Go code itself stays.

---

## Phase 0 — Open question

Almost everything is locked above. The one remaining decision:

- **Customer onboarding flow.** Existing Bicep creates the control plane *and* the data plane. New flow:
  - Customer-side Bicep creates the app registration + role assignments + data-plane scaffolding (managed environment, networking) only.
  - Credentials land in the enclave; Datadog creates the `AppRegistrationIntegration` record.
  - First invocation of the LFO crawlers brings up the actual forwarder Container App Jobs.
  Confirm who owns the Bicep simplification and the exact shape of the new install.

---

## Phase 1 — Skeleton

Goal: three LFO crawlers compile, register, and run as no-ops in the platform.

- Create `apps/generic-crawler/internal/jobs/lfo/` with:
  - `resources_crawler.go` — implements `Crawler` (`ID() = "azure_lfo_resources"`, `Crawl()` returns nil).
  - `scaling_crawler.go` — `azure_lfo_scaling`.
  - `diagnostic_settings_crawler.go` — `azure_lfo_diagnostic_settings`.
- Each has a `New...Crawler(ctx, bcfg, ccfg)` factory and a `Register...Crawler()` function, mirroring `active_directory_crawler.go`.
- Add the three `Register*` calls to `apps/generic-crawler/internal/registrations/registrations.go:RegisterAll()`.
- BUILD.bazel updates so the package compiles in CI.
- Verify the platform schedules them and they show up in dashboards as no-op runs.

---

## Phase 2 — Port resources crawler

Goal: replace `control_plane/tasks/resources_task.py` and its supporting code in Go.

- New crawler: `azure_lfo_resources` (Phase 1 skeleton). One invocation per (org, subscription). Wires up:
  - Subscription-scoped Azure SDK client for resource enumeration, mirroring the Python `ResourceClient`. The subscription comes from `ccfg.SubscriptionIntegrations[0]` (the framework guarantees the list shape; in practice each invocation gets one subscription).
  - `MakeCredentials` for auth.
  - Tag filtering via `integration.GetTagFilters(...)`.
  - Resource-type filtering currently in `tasks/constants.py` (allowed/nested resource types) — port to a Go file in the same package.
  - Cache shape currently in `cache/resources_cache.py` — translate the per-region resource dict into Go structs and persist via `bcfg.KV` keyed on (orgID, subscriptionID, "lfo_resources").
- Drop `MONITORED_SUBSCRIPTIONS` plumbing entirely — the platform passes the subscription via `ccfg`. `cache/resources_cache.py:deserialize_monitored_subscriptions` does not get ported.
- Drop the `include` flag and the `is_control_plane_storage_account` exclusion logic — irrelevant once lfostorage is gone.
- Keep the `IGNORED_LFO_PREFIXES` filter, but trimmed to just the forwarder data-plane prefixes that still exist in customer Azure: `dd-log-forwarder-` (covers both Container App Jobs and `dd-log-forwarder-env-*` managed envs) and `ddlogstorage`. The function-app prefixes (`scaling-task-`, `resources-task-`, `diagnostic-settings-task-`) come out — those resources no longer exist in customer Azure. Without this filter the diagnostic-settings crawler would create diag settings on the per-forwarder `ddlogstorage*` accounts, sending forwarder-internal logs back through the forwarder.
- Cache schema versioning (V1→V2 upgrade in `resources_cache.py`) is dropped — clean break, no V1 caches to migrate.

---

## Phase 3 — Port diagnostic-settings crawler

Goal: replace `control_plane/tasks/diagnostic_settings_task.py`.

- One invocation per (org, subscription).
- Reads the resources cache written in Phase 2 (`bcfg.KV` lookup keyed on (org, subscription)).
- Reads the assignments cache for the same (org, subscription) that scaling produces in Phase 4 — sequence so this crawler degrades gracefully if assignments aren't yet written.
- For each (resource, region, assigned forwarder) in this subscription, creates/updates Azure diagnostic settings pointing at the forwarder's storage account. Reuse the SDK call patterns from `tasks/client/diagnostic_settings_client.py`.
- Persist the settings-event cache (today `cache/diagnostic_settings_cache.py:settings_event.json`) via `bcfg.KV`, keyed on (org, subscription).

---

## Phase 4 — Port scaling crawler and `log_forwarder_client`

Goal: replace `control_plane/tasks/scaling_task.py` plus the bulk of `control_plane/tasks/client/log_forwarder_client.py`.

- One invocation per (org, subscription). The crawler operates over all forwarders in this subscription, fanning out across regions internally.
- Reads each forwarder's `metrics_{datetime}.json` from its per-forwarder `ddlogstorage{config_id}` account (Azure Blob SDK in Go, connection-string flow same as today).
- Decides scaling actions; creates/scales/deletes Container App Jobs in this subscription (Go Azure SDK `armappcontainers`).
- Creates Container Apps managed environments per region on demand (port `log_forwarder_client.py:create_log_forwarder_managed_environment` — managed envs are not pre-provisioned by customer-side Bicep).
- Creates the per-forwarder storage account when needed (`ddlogstorage{config_id}`), with the same lifecycle-management policy that `create_log_forwarder_storage_management_policy` sets today.
- Writes the assignments cache via `bcfg.KV` keyed on (org, subscription). Schema unchanged from `cache/assignment_cache.py`, just expressed as Go structs.
- Owns `config_id` minting — fresh random 12-char hex id per forwarder on scale-up (port `tasks/common.py:generate_unique_id` to Go). The id space is per (org, subscription) in `bcfg.KV`; collisions are vanishingly unlikely at 12 hex chars.
- Forwarder secrets/env vars (DD_API_KEY, FORWARDER_IMAGE, PII rules, etc.) come from the integration record and `bcfg.Config` — not env vars.

---

## Phase 5 — Customer-side scaffolding (in azure-log-forwarding-orchestration)

Goal: shrink the in-customer install to the app registration and its role assignments. Everything in the data plane (managed envs, per-forwarder storage accounts, Container App Jobs) is created on demand by the scaling crawler.

- Rework the existing Bicep templates so a new install provisions only:
  - The app registration (or instructions to create one).
  - Role assignments on each monitored subscription (see below).
  - A resource group for forwarder data-plane resources to live in, if the crawler doesn't create that itself.
- App-registration role assignments needed on each monitored subscription:
  - **Storage Account Contributor** (`17d1049b-9a84-46fb-8f53-d4f1a5c1c5e1`) — full lifecycle of per-forwarder `ddlogstorage{config_id}` accounts (create, delete, container/policy management) and grants `listKeys` for the metrics read path.
  - **Container Apps roles** to create/scale/delete forwarder Container App Jobs *and* managed environments — match what the function-app MI is granted today.
  - **Monitoring Contributor** (or equivalent) for creating/removing diagnostic settings on monitored resources.
  - **Reader** at the subscription scope for resource enumeration (`resources.list` calls in the resources crawler).
- Remove from the templates: lfostorage account, app service plan, the four function apps, deployer setup, role assignments tagged `ddlfo*` for the function-app MI.
- Update README and CONTRIBUTING.

---

## Phase 6 — Tear down the Python control plane

Goal: delete every piece of the old control plane once the Go crawlers are running for real customers.

In `azure-log-forwarding-orchestration`:

- Delete `control_plane/` entirely: `tasks/`, `cache/`, `config/`, `scripts/publish.py`, `scripts/uninstall.py`.
- Delete `scripts/run_task.sh`.
- Delete the lfostorage filter in `forwarder/internal/logs/client.go:61-65` and the corresponding test in `forwarder/internal/logs/client_test.go:82-95` (was AZINTS-4446).
- Delete the build/CI pipeline that produces `resources_task.zip`, `scaling_task.zip`, `diagnostic_settings_task.zip` and uploads them to `ddazurelfo`. Decommission the `ddazurelfo` storage account itself if it's not used by anything else.
- Update CI to stop running the Python tests under `control_plane/tests/`.

This phase is gated on the Go crawlers being in production for new customers (clean break — no migration of existing deployments to coordinate).

---

## Phase 7 — Final cleanup

- Grep pass in `azure-log-forwarding-orchestration`: `lfostorage`, `CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX`, `STORAGE_ACCOUNT_URL`, `PUBLIC_STORAGE_ACCOUNT_URL`, `AzureWebJobsStorage` (only forwarder/data-plane references should remain), `manifest`, `deployer`, `function.json`, `host.json`, references to the function-app naming prefixes (`SCALING_TASK_PREFIX`, `RESOURCES_TASK_PREFIX`, `DIAGNOSTIC_SETTINGS_TASK_PREFIX`, `dd-lfo-control-`).
- Update top-level README to describe the new architecture: data plane in customer Azure, control plane as Datadog-side crawlers, app-registration handoff during onboarding.
- Delete `control-plane-crawler-migration.md` and this plan once the work is done.

---

## What stays untouched

- `forwarder/` Go code (data plane). Only the lfostorage filter is removed in Phase 6.
- The forwarder image build and registry publish (`datadoghq.azurecr.io/forwarder:latest`).
- Per-forwarder `ddlogstorage{config_id}` storage account model and the `metrics_*.json` blob format — Phase 4 reads it the same way today's scaling task does, just in Go.
- Schemas in `cache/*.py` — the *shapes* survive as Go structs; only the storage backend underneath changes.
- Filtering rules, PII scrubbing, resource-type allow-lists — these get translated, not redesigned.

---

## Phasing dependencies

```
Phase 0 (decisions)
   ↓
Phase 1 (skeleton)
   ↓
Phase 2 (resources) ──→ Phase 3 (diagnostic settings)
   ↓                       ↓
Phase 4 (scaling) ─────────┘
   ↓
Phase 5 (customer scaffolding)  [can start in parallel with 2-4]
   ↓
Phase 6 (delete Python control plane)  [gated on 2-5 in production]
   ↓
Phase 7 (final cleanup)
```

Phase 5 has no dependency on the Go ports and can run in parallel with phases 2-4 by whoever owns the Bicep. Phases 2 and 3 share state (resources cache → diagnostic-settings consumer) so 3 lags 2; phase 4 is independent of 3 but shares conventions, so doing 2 first as the template is sensible.
