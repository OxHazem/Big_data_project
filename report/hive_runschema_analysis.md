# 🔍 `run_schema.sh` Failure Analysis

## TL;DR

The script itself is logically correct. The real problem is that **HiveServer2 is crashing in a retry loop and never binds to port 10000**, so Beeline always gets `Connection refused`. This is caused by the metastore being misconfigured — the `HIVE_CORE_CONF_javax_jdo_option_ConnectionURL` environment variable is silently **never written** to `hive-site.xml`.

---

## Root Cause Chain

### ❌ Issue 1 — HiveServer2 is NOT listening on port 10000 (the actual fatal failure)

Even though the container shows `Status: Up`, HiveServer2 (the Thrift/JDBC server) is **not bound to port 10000**. Running `netstat -tlnp` inside the container shows only one listener on `127.0.0.11:42299` (Docker's internal DNS resolver). Port 10000 is completely absent.

From `/tmp/root/hive.log` inside the container:
```
ERROR [main]: metastore.RetryingHMSHandler - MetaException(message:Version information not found in metastore.)
ERROR [main]: metastore.RetryingHMSHandler - HMSHandler Fatal error
WARN  [main]: server.HiveServer2 - Error starting HiveServer2 on attempt 19, will retry in 60000ms
```

HiveServer2 enters an endless retry loop (60s between attempts) and **never successfully opens** the Thrift port. The JVM process stays alive (which fools `docker inspect`), but the service is dead.

---

### ❌ Issue 2 — `HIVE_CORE_CONF_*` env vars are silently ignored by the entrypoint

Your `docker-compose.yml` sets:
```yaml
environment:
  - HIVE_CORE_CONF_javax_jdo_option_ConnectionURL=jdbc:postgresql://hive-metastore:5432/metastore
```

The entrypoint script (`/entrypoint.sh`) in `bde2020/hive` processes env vars with the prefix pattern like `CORE_CONF_*` → writes to `core-site.xml`, `HDFS_CONF_*` → writes to `hdfs-site.xml`. The **Hive-specific** settings need to use the prefix `HIVE_SITE_CONF_*` to be written to `/opt/hive/conf/hive-site.xml`.

Because of this, **`hive-site.xml` remains completely empty** (`<configuration></configuration>`), meaning HiveServer2 tries to connect to its **embedded Derby metastore** instead of PostgreSQL, finds no schema (`Table/View 'DBS' does not exist.`), and crashes.

---

### ❌ Issue 3 — `set -euo pipefail` causes premature exit before the real error is logged

The script uses `set -euo pipefail` at line 8. Because Beeline exits non-zero on connection failure, bash would normally abort the script and swallow subsequent errors. You only see it because `run_schema.sh` wraps the beeline call in an `if` statement — but any future errors in the pipeline would exit silently.

---

## The Fix

### Step 1 — Fix the `HIVE_SITE_CONF_*` env var prefix in `docker-compose.yml`

```diff
  hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    container_name: hive-server
    environment:
-     - HIVE_CORE_CONF_javax_jdo_option_ConnectionURL=jdbc:postgresql://hive-metastore:5432/metastore
+     - HIVE_SITE_CONF_javax_jdo_option_ConnectionURL=jdbc:postgresql://hive-metastore:5432/metastore
+     - HIVE_SITE_CONF_javax_jdo_option_ConnectionDriverName=org.postgresql.Driver
+     - HIVE_SITE_CONF_javax_jdo_option_ConnectionUserName=hive
+     - HIVE_SITE_CONF_javax_jdo_option_ConnectionPassword=hive
      - SERVICE_PRECONDITION=hive-metastore:5432
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
```

> [!IMPORTANT]
> The `bde2020/hive` image uses `HIVE_SITE_CONF_` (not `HIVE_CORE_CONF_`) to write properties into `hive-site.xml`. Using the wrong prefix means nothing gets written and the metastore connection defaults to Derby.

### Step 2 — Restart the Hive stack cleanly

```bash
# From the project root
docker compose down hive-server hive-metastore
docker compose up -d hive-metastore
# Wait ~10s for metastore PostgreSQL to be ready
sleep 10
docker compose up -d hive-server
# Wait for HiveServer2 to initialize (it can take 30-60s)
sleep 60
```

### Step 3 — Verify port 10000 is bound before running the schema

```bash
# Should show a listener on *:10000
docker exec hive-server netstat -tlnp | grep 10000
```

### Step 4 — Run the schema

```bash
bash "src /hive/run_schema.sh"
```

---

## Summary Table

| # | Problem | Location | Fix |
|---|---------|----------|-----|
| 1 | HiveServer2 never binds port 10000 | `hive-server` container | Fix env var prefix (Issue 2) |
| 2 | Wrong env var prefix `HIVE_CORE_CONF_` | `docker-compose.yml` line 77 | Change to `HIVE_SITE_CONF_` |
| 3 | Empty `hive-site.xml` → Derby fallback | Container filesystem | Result of Issue 2 |

---

> [!NOTE]
> The `run_schema.sh` script itself is well-written. The bug is entirely in `docker-compose.yml`. Once HiveServer2 successfully starts and listens on 10000, the script should deploy the schema correctly.
