// Package dbapi exposes read-only Go clients for the two SQLite
// databases owned by Yoke Core: the plugin registry and the log store.
//
// External Go applications use these clients to observe Yoke state
// without going through yoke-core itself. The clients open the
// database files in mode=ro, so they are safe to run alongside a
// running Core (which keeps the file in WAL mode).
//
// Schema management lives in yoke-core (internal/persistence and its logstore/registry schema packages).
// dbapi only reads tables produced by that schema; the types it
// exposes form a stable public API and are intentionally decoupled
// from the internal write-path types.
//
// Sub-packages:
//
//   - dbapi/registry — typed read client for registry.db
//   - dbapi/logstore — typed read client for logs.db
package dbapi
