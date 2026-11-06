---
name: airflow-promote-config
description: Compare and copy Apache Airflow 2.x variables, pools and connections between environments (e.g. staging → prod) with airflow-cli profiles. Plan with a diff first, keep per-environment values, apply through a deliberate write profile, then verify. Use when asked to sync, promote, copy or diff Airflow config between servers, or to set up a new environment from an existing one.
---

# Promote config between environments

Needs `airflow-cli` and `jq`, and one profile per environment in `~/.airflow.toml`. The recommended
setup gives production two profiles: a `read_only` one for everyday use, and one that can write,
used only when you mean to change something.

```toml
username = "admin"

[profiles.staging]
base_url = "https://airflow.staging.example.com"
password = "..."

[profiles.prod]                 # everyday: looking only
base_url = "https://airflow.example.com"
password = "..."
read_only = true

[profiles.prod-write]           # deliberate changes
base_url = "https://airflow.example.com"
password = "..."
```

## Steps

1. **Plan.** Export both sides, limit to the keys you manage, drop the ones that should differ per
   environment (URLs, environment names), and `diff`. List pools and connections that exist in
   one environment but not the other.
2. **Show the plan** before changing anything. The read-only profile can't apply it by accident;
   it refuses with exit 5.
3. **Apply** with the write profile. Variables go through `variables import`, pools through `pools set`.
   Connections are copied without their passwords, which the API never returns, so you supply the
   target's secret.
4. **Verify** by running the diff again; it should be empty.

## Worked example

Staging has new settings, a `warehouse` pool and a `warehouse` connection that prod doesn't have
yet. `cli_demo_env` should stay different in each environment.

```bash
FROM=staging
TO=prod             # read-only profile, for looking
TO_WRITE=prod-write # same server, writes allowed; used only for the apply step
KEEP_PER_ENV='["cli_demo_env"]'

# 1. Plan
pick() { jq -S --argjson keep "$KEEP_PER_ENV" \
  'with_entries(select((.key | startswith("cli_demo_")) and (.key | IN($keep[]) | not)))'; }
airflow-cli -p "$FROM" variables export | pick > from-vars.json
airflow-cli -p "$TO"   variables export | pick > to-vars.json
diff to-vars.json from-vars.json && echo "variables already in sync"

comm -13 <(airflow-cli -p "$TO"   pools list --all --compact | jq -r '.pools[].name' | sort) \
         <(airflow-cli -p "$FROM" pools list --all --compact | jq -r '.pools[].name' | sort) | sed 's/^/missing pool: /'
comm -13 <(airflow-cli -p "$TO"   connections list --all --compact | jq -r '.connections[].connection_id' | sort) \
         <(airflow-cli -p "$FROM" connections list --all --compact | jq -r '.connections[].connection_id' | sort) | sed 's/^/missing connection: /'
```

```
2c2
<   "cli_demo_settings": "{\"batch_size\": 100}"
---
>   "cli_demo_settings": "{\"batch_size\":500,\"regions\":[\"eu\",\"us\"]}"
missing pool: warehouse
missing connection: warehouse
```

```bash
# 2. The everyday prod profile refuses to change anything
airflow-cli -p "$TO" variables import from-vars.json; echo "exit=$?"
```

```
error: setting cli_demo_settings: profile 'prod' is read-only: refusing PATCH /api/v1/variables/cli_demo_settings because it would change server state
hint: read-only comes from `read_only` in ~/.airflow.toml (top level or the profile), --read-only, or AIRFLOW_CLI_READ_ONLY
exit=5
```

```bash
# 3. Apply with the write profile
airflow-cli -p "$TO_WRITE" variables import from-vars.json

airflow-cli -p "$FROM" pools get warehouse --compact | jq -r '"\(.slots)\t\(.description)"' \
  | while IFS=$'\t' read -r slots desc; do
      airflow-cli -p "$TO_WRITE" pools set warehouse "$slots" --description "$desc" --compact | jq -c '{name, slots}'
    done

airflow-cli -p "$FROM" connections get warehouse --compact \
  | jq 'del(.connection_id) | with_entries(select(.value != null))' \
  | airflow-cli -p "$TO_WRITE" connections create warehouse --json - \
      --conn-password "$PROD_WAREHOUSE_PASSWORD" --compact \
  | jq -c '{connection_id, host, schema}'

# 4. Verify
airflow-cli -p "$TO" variables export | pick | diff - from-vars.json && echo "variables in sync"
airflow-cli -p "$TO" variables get cli_demo_env --value      # per-env value left alone
```

```
imported 1 variables
{"name":"warehouse","slots":8}
{"connection_id":"warehouse","host":"warehouse.internal","schema":"analytics"}
variables in sync
prod
```

## Variations

- **Everything, not just one prefix:** drop the `startswith` filter. Keep the `KEEP_PER_ENV` list.
- **Back up before applying:** `airflow-cli -p prod variables export > prod-vars-$(date +%F).json`. To restore,
  pipe that file into `variables import` on the write profile.
- **From a file in git:** keep `variables.json` in the repo and `variables import` it into each
  environment, instead of copying between servers.
- **Connection that already exists:** use `connections update ID --host ...`. Only the fields you pass change,
  and the password stays.

## Pitfalls

- `variables import` creates or overwrites; it never deletes. To remove keys that exist only in the
  target, check `comm -23` on the key lists, then `variables delete` them deliberately.
- Variable values are strings. JSON values come back as JSON text, so compare exports, not raw files.
- Passwords and other secrets in a connection's `extra` are copied as-is. Check the `extra` field before
  promoting it to production.
- Variables, connections and pools are only what the API shows. A secrets backend (Vault, AWS SM and
  so on) isn't visible here and has to be synced separately.
