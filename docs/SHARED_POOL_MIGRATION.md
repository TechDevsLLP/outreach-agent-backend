# Shared company/prospect pool migration

This runbook preserves exactly the approved customer-data scope: canonical
`companies` and `prospects`. It does **not** migrate campaigns, enrollments,
messages, conversations, notifications, usage records, or other outreach
history. Tenant-specific fields discovered on canonical prospects are split
into `prospect_state` only when one account is provable. Ambiguous ownership,
campaign fit, outreach history, unknown fields, conflicting duplicates, and
broken references are retained in an encrypted operator quarantine artifact,
not guessed into production. Operational pipeline state (`stage`, enrichment
run/status/error/timestamps, prefilter and title-gate decisions,
disqualification, and optimal-send-time recommendations) is also quarantined;
it is recomputed after cutover and is not a factual person attribute.

The migration is a new-database cutover. It never rewrites the source and the
restore command refuses a non-empty target. Rollback is therefore a database
configuration switch, not a reverse transformation.

## Safety model

- The source Atlas user must have read-only access to only `companies` and
  `prospects`. Do not give the export process a write-capable URI.
- MongoDB credentials are read from `OUTFLO_MIGRATION_MONGODB_URI`; they do not
  appear in shell history or the process command line.
- Export and bundle directories are created with mode `0700`; artifacts and
  manifests use `0600`. They contain PII. Store them in an encrypted,
  versioned, access-logged bucket and set a short quarantine retention period.
- Every logical collection has a count and SHA-256 over canonical Extended
  JSON. Export performs a second ordered source pass and fails if a count or
  hash moved; transform validates the export manifest again.
- Restore is disabled by default, requires a token derived from the exact
  bundle, requires an empty target database, and rechecks counts and hashes
  after insert and index creation.
- The script has no update, replace, delete, drop, or cleanup operation against
  a source database. A partial target is abandoned and replaced with another
  empty database name.

## 1. Rehearse before the change window

Use a disposable Atlas project or cluster. Create separate source-read and
target-write users, restrict their IP access, enable Atlas backups/PITR on the
source, and record the source cluster/database and intended target database in
the change ticket.

From `backend/`, first preview the export. This does not connect:

```bash
python3 scripts/shared_pool_migration.py export \
  --database outflo_source \
  --output /secure/outflo-export-YYYYMMDD
```

Then expose the read-only URI only to the process environment and perform the
scoped export:

```bash
read -rsp 'Read-only Atlas URI: ' OUTFLO_MIGRATION_MONGODB_URI
export OUTFLO_MIGRATION_MONGODB_URI
python3 scripts/shared_pool_migration.py export \
  --database outflo_source \
  --output /secure/outflo-export-YYYYMMDD \
  --execute-read
unset OUTFLO_MIGRATION_MONGODB_URI
```

Copy the untouched export to encrypted immutable storage before transforming
it. Record the object version, encryption key, export manifest, and SHA-256 in
the change ticket.

## 2. Inventory and approve exceptions

```bash
python3 scripts/shared_pool_migration.py inventory \
  --input /secure/outflo-export-YYYYMMDD \
  --output /secure/outflo-inventory-YYYYMMDD.json
```

Review and sign off:

- collection counts and observed field/type distribution;
- missing canonical identities;
- duplicate normalized LinkedIn/email/domain/UID groups;
- every nested tenant-leakage path and ambiguous owner count;
- broken `prospects.company_id` references.

Do not continue if source counts differ from Atlas, the export manifest fails,
or the inventory includes a field whose classification is not understood.

## 3. Build and validate the bundle

The output path must not exist. This prevents accidental replacement of a
previously approved bundle.

```bash
python3 scripts/shared_pool_migration.py transform \
  --input /secure/outflo-export-YYYYMMDD \
  --output /secure/outflo-bundle-YYYYMMDD

python3 scripts/shared_pool_migration.py validate \
  --bundle /secure/outflo-bundle-YYYYMMDD
```

Review `manifest.json` counts and `quarantine_reasons`. Sample every quarantine
reason. `quarantine.jsonl.gz` is an operator artifact and is never restored.
The bundle's `prospect_state` contains only tenant residue separated from a
canonical record with one proven account. Re-created launch accounts must use
the same account IDs before those overlays are exposed; otherwise keep the
overlays offline and start with empty tenant state.

Acceptance gates:

- bundle validation passes without edits;
- zero tenant fields exist on output companies/prospects;
- normalized identity keys and `_id` values are unique;
- all output prospect company references resolve;
- all overlay prospect references resolve and `(account_id, prospect_id)` is
  unique;
- quarantine disposition and retention are approved.

## 4. Restore only to an empty staging target

Preview validates the bundle and prints the exact confirmation token without
connecting:

```bash
python3 scripts/shared_pool_migration.py restore \
  --bundle /secure/outflo-bundle-YYYYMMDD \
  --database outflo_stage_restore
```

Set a write URI for the disposable target and use the token in the bundle
manifest:

```bash
read -rsp 'Target Atlas URI: ' OUTFLO_MIGRATION_MONGODB_URI
export OUTFLO_MIGRATION_MONGODB_URI
python3 scripts/shared_pool_migration.py restore \
  --bundle /secure/outflo-bundle-YYYYMMDD \
  --database outflo_stage_restore \
  --execute \
  --confirmation RESTORE:<bundle-token>
unset OUTFLO_MIGRATION_MONGODB_URI
```

The command creates only the three runtime collections, required uniqueness
indexes, and a migration receipt. Start the current backend against staging so
`database.create_indexes()` installs the remaining query indexes. Inspect every
index before exercising application traffic.

After account/auth configuration is seeded separately, run the two-tenant
authorization suite and verify company/prospect list/detail, campaign-scoped
scoring, enrichment, and pagination. Compare Atlas counts with `manifest.json`
and independently recompute a sample of hashes. No production cutover is
approved until this restore drill and application test are attached to
`IMPLEMENTATION_LEDGER.md`.

## 5. Production cutover

1. Pause canonical company/prospect ingestion and wait for active enrichment
   jobs to finish or checkpoint. Campaign sends may remain stopped because their
   history is outside migration scope.
2. Repeat export, inventory, transform, quarantine review, and validation with
   fresh timestamped paths. Never reuse rehearsal artifacts.
3. Restore to a new empty production database name.
4. Seed required auth/account configuration separately; preserve account IDs
   only for overlays that were explicitly approved.
5. Point staging application instances at the new database, install/inspect all
   indexes, run launch gates, and record counts/hashes.
6. Change the production database secret to the new database, restart API and
   scheduler, verify health and read journeys, then resume ingestion gradually.
7. Keep the old database read-only and PITR-enabled through the rollback window.

## Rollback and failed restore

If restore, index creation, validation, or smoke testing fails, do not repair
the target in place. Leave production configured to the old database, revoke
the failed target credentials, preserve its logs for diagnosis, and repeat into
a new empty database name.

If a post-cutover launch gate fails, pause workers and sends, switch the
database secret back to the old database, restart API/scheduler, and re-run
health plus read-only company/prospect checks. Because writes were paused and
the old database was never changed, this is the authoritative rollback. Any
new writes made after cutover must be inventoried separately; never merge them
automatically into the old shared pool.

Do not delete the source database or immutable export until the rollback window
has closed, a second restore drill has passed, and the data owner approves
quarantine destruction.
