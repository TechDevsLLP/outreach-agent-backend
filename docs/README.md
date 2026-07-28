# OutFlo system documentation

Maintained contracts for the FastAPI, MongoDB, provider and EC2 launch
architecture. These documents distinguish implemented foundations from
release-proven behavior.

| Document | Contents |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Guided-autopilot flow, tenant/shared-pool boundaries, durable jobs/sends, scheduler, conversations, notifications and EC2 topology |
| [API.md](API.md) | Maintained launch API and authorization contract; development OpenAPI is the exact route inventory |
| [DATABASE.md](DATABASE.md) | Shared pool + account/campaign overlays, durable jobs/send attempts, canonical identities and restore/index rules |
| [INTEGRATIONS.md](INTEGRATIONS.md) | Launch provider matrix and OAuth, ownership, retry, reconciliation, cap and webhook contracts |
| [SHARED_POOL_MIGRATION.md](SHARED_POOL_MIGRATION.md) | Scoped company/prospect export, tenant-neutral transform, quarantine, empty-target restore, validation, and rollback |
| [../deploy/README.md](../deploy/README.md) | EC2 operations, load envelope, backups/restore, SLOs and exact no-go gates |
| [../loadtest/README.md](../loadtest/README.md) | Provider-free 100-account/three-seat API and SSE load harness |

`claude-memory/` contains historical notes and is not an architecture or release
authority. Production release evidence belongs in the root
`IMPLEMENTATION_LEDGER.md`.
