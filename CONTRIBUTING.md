# Contributing

Thanks for considering a contribution.

## Development setup

Requirements: Node.js >= 22, npm, and a local `redis-server` (the test suite starts its
own private, socket-only instance; `/usr/bin/redis-server` must be installed).

```bash
git clone https://github.com/rahulrajaram/gptqueue.git
cd gptqueue
npm install          # runs the build via `prepare`
npm test             # full vitest suite
npm run test:py      # Python installer tests
```

`node-pty` is a native dependency. If your npm blocks install scripts, run
`npm rebuild node-pty` before the PTY/live tests.

## Checks before opening a pull request

```bash
npm run build
npx tsc --noEmit
npx oxlint src/
npm test
npm run test:py
```

CI runs the same steps on Node 22 and 24 (`.github/workflows/ci.yml`). Live acceptance
tests are skipped unless `GPTQUEUE_LIVE_ACCEPTANCE=1`; do not enable them in CI.

## Commit and review conventions

- Keep commits focused; use a conventional subject (`fix:`, `feat:`, `docs:`, `test:`, `chore:`).
- Do not bypass hooks (`--no-verify`) and do not commit harnesses, run receipts, session
  logs, or agent-state files.
- Tests should be hermetic: start from a clean clone, use a private Redis, and never
  depend on a developer's home directory or a running shared server.
- Report security issues via `SECURITY.md`, not a public issue.

## Licence

By contributing you agree that your contribution is licensed under the repository's MIT
licence (see `LICENSE`).
