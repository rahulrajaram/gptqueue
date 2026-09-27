# OpenCode integration

GPTQueue's OpenCode integration is a host plugin. It binds one private
GPTQueue MCP client and Redis session to each native OpenCode `sessionID` and
`directory`. A native `Task` child therefore gets its own GPTQueue identity
even when OpenCode reuses the parent process or MCP connection.

The plugin is built from this repository with:

```sh
npm run build
```

The local plugin entrypoint is:

```text
/path/to/gptqueue/dist/registered-shell/opencode-plugin.js
```

The default export requires an explicit Redis URL from the plugin option
`redisUrl`, `GPTQUEUE_REDIS_URL`, or `REDIS_URL`. The integration has no db0
fallback. For qualification, use a private database such as
`redis://127.0.0.1:6379/15`.

Each session backend registers a deterministic `gptqueue-opencode-<sessionID>`
mailbox, starts the existing inbox dispatcher, and keeps that ownership alive
through native idle events. The dispatcher submits `prompt_async` only after
the exact session is verified and idle. The OpenCode model claims work,
sends results or errors, renews claims when needed, and acknowledges only
after delivery. A timeout or native rejection is reconciled from the exact
native user message and deterministic `msg_<sha256(operation_id)>`; it is
never blindly resubmitted.

Native tools are exposed with a `gptqueue_` prefix and forward through the
session's bound MCP client. They include claim, acknowledge, renew, send,
receive, queue status, runtime status, peer discovery, agent details, and
delivery diagnostics. The plugin removes `session_id` from bound argument
schemas and does not expose `register_agent`, `bind_runtime`, or registration
credentials. The system guidance follows the already-registered MCP
connection model: discovery is advisory, ambiguous peers are not routed
automatically, and destructive receive is explicit.

The public GPTQueue MCP schemas remain unchanged. `runtimeBindingSchema` still
accepts only its public `codex` and `pi` values; OpenCode identity is an
internal host binding. OpenCode lifecycle events create sessions, delete
sessions only on `session.deleted`, and dispose all retained sessions on the
supported `server.instance.disposed` event. Idle and error events do not close
ownership.

Use an isolated OpenCode profile for qualification. This keeps the test away
from global and project configuration, disables the old shared GPTQueue MCP
entry in the same profile, and does not alter any installed configuration:

```sh
set -eu

REPO=/path/to/gptqueue
PROFILE=$(mktemp -d)
mkdir -p "$PROFILE/home" "$PROFILE/config/opencode" "$PROFILE/data" "$PROFILE/cache" \
  "$PROFILE/state" "$PROFILE/work"

cat >"$PROFILE/config/opencode/opencode.json" <<JSON
{
  "plugin": ["file:///path/to/gptqueue/dist/registered-shell/opencode-plugin.js"],
  "mcp": {
    "gptqueue-shared": {
      "type": "local",
      "command": ["/bin/false"],
      "enabled": false
    }
  }
}
JSON

(
  cd "$PROFILE/work"
  XDG_CONFIG_HOME="$PROFILE/config" \
  XDG_DATA_HOME="$PROFILE/data" \
  XDG_CACHE_HOME="$PROFILE/cache" \
  XDG_STATE_HOME="$PROFILE/state" \
  HOME="$PROFILE/home" \
  GPTQUEUE_REDIS_URL=redis://127.0.0.1:6379/15 \
  opencode
)
```

The private `HOME` is required by installed OpenCode builds that also inspect
the legacy `~/.opencode` location; XDG variables alone do not isolate that
lookup. Do not set `OPENCODE_PURE=1` for this integration: the installed
binary suppresses configured plugin tools in that mode.

The disabled `gptqueue-shared` entry is deliberate. Do not load the old
shared GPTQueue MCP in the same OpenCode profile: it creates a second
connection that can compete for registration and exposes an unbound tool
surface. If a profile uses a different old entry name, disable that exact
entry in the owned profile as well. Keep provider, OAuth, and other secret
configuration outside this test profile.

The focused source checks are:

```sh
npx tsc --noEmit
REDIS_URL=redis://127.0.0.1:6379/15 npx vitest run \
  tests/opencode-runtime.test.ts \
  tests/opencode-plugin.test.ts \
  tests/opencode-sessions.test.ts \
  tests/opencode-backend.test.ts
```

This source integration does not launch an offline OpenCode process, change a
running daemon, or wake a session that the host has deleted. A plugin loaded
by an already-running OpenCode process keeps the old module until that process
is restarted or reloads its plugin set. Existing Codex, Pi, Claude, and
Gemini-managed profiles are outside this integration; preserve their owners'
configuration and permissions and qualify OpenCode in an owned profile.
