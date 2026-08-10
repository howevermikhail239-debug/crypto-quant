# Project backup inventory

Актуально на 2026-08-10. Этот файл перечисляет обнаруженные Git-проекты в
`C:\Users\Admin\Documents` и `C:\Users\Admin\Desktop`.

| Project | Local path | Branch | Checkpoint commit | Validation |
|---|---|---|---|---|
| Crypto Quant | `C:\Users\Admin\Documents\ChatGPT\анализ крипты` | `master` | `11bd074834a5735c218b91ecc87bc087677ff3e5` | 91 pytest passed; Ruff, config and uv lock checks passed |
| Рядом / МикроУслуги | `C:\Users\Admin\Documents\ChatGPT\МикроУслуги` | `master` | `21e65309b57927a6a283ee4f43898207257791f6` | ESLint, TypeScript and production Next.js build passed |
| Promo Hunter / Поиск промокодов | `C:\Users\Admin\Documents\ChatGPT\Поиск промокодов` | `master` | `817b1716e601352f06fedee257c86262d68dbf49` | 72 pytest passed; Ruff passed |

## Restore prerequisites

### Crypto Quant

- Python 3.12 and `uv`.
- Run `uv sync --locked --group dev`.
- Market data is intentionally external at `C:\crypto_quant_data` and is not in Git.
- Continue from `HANDOFF.md`.

### Рядом / МикроУслуги

- Node.js 22, npm, Docker/Compose for the container path, and PostgreSQL/PostGIS.
- Run `npm ci`, copy an appropriate `.env.*.example`, supply new secrets, apply Prisma migrations, then run `npm run build` or Docker Compose.
- Deployment details are in `docs/FREE_DEPLOYMENT_GUIDE.md`.
- The previous Telegram bot token embedded in the local Dockerfile was removed before the first commit. Rotate it before deployment.

### Promo Hunter / Поиск промокодов

- Python 3.11+, a virtual environment, Playwright browsers, and optional PostgreSQL.
- Install with `pip install -e ".[dev,postgres]"`, copy `.env.example`, supply new secrets, and follow `README.md`.
- Local `.env`, SQLite DB, browser profiles, logs, PID and generated source bundles are intentionally not in Git.

## Remote backup status

At the time of this inventory, none of the three repositories had a Git remote.
Local commits protect history from accidental edits but do not protect against
loss of the local disk. Create private remote repositories and push every branch
and tag before considering the backup complete.

Suggested verification after configuring remotes:

```powershell
git remote -v
git push -u origin master
git ls-remote --heads origin
```

Do not commit real `.env` files, credentials, databases, browser profiles, logs,
runtime market data, or generated source-export archives.
