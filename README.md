# Wematics Archive Dashboard

Full-stack dashboard for collecting Wematics sky camera data, indexing local and FTP archives, comparing sources, and running background download, upload, transfer, verify, and inventory jobs.

## Stack

- Backend: FastAPI, SQLAlchemy, SQLite by default
- Frontend: React 18, React Router, Vite, Recharts
- Jobs: in-process worker and scheduler backed by the application database
- Deployment: Docker Compose with Nginx in front of the frontend bundle

## What The App Does

- Browses remote Wematics cameras, variables, dates, files, previews, and CSV samples
- Indexes local archive folders and exposes date/file inventory views
- Tracks FTP uploads and supports direct FTP directory browsing and file download
- Compares `remote`, `local`, and `ftp` inventories to find missing dates and partial days
- Runs queued background jobs for download, upload, transfer, verify, and local inventory scans
- Stores job history, per-job events, file audit events, schedules, glossary data, and CSV analysis cache in the database

## Repository Layout

```text
backend/
  app/
    api/
    core/
    db/
    models/
    schemas/
    services/
    workers/
  migrations/
  tests/
frontend/
  src/
docker-compose.yml
```

## Runtime Paths

Local backend runs from `backend/`, so the default paths resolve to:

- Database: `backend/data/wematics.db`
- Local archive: `backend/downloads/`
- Transfer temp workspace: `backend/data/transfer_tmp/`

Docker Compose overrides the archive path so uploads and downloads use the repository-level `downloads/` directory inside the container.

These runtime folders are local-only and should stay out of version control:

- `.env`
- `backend/data/`
- `data/`
- `downloads/`
- `output/`

## Environment

Copy `.env.example` to `.env` and fill in the values you need.

`.env` is for local runtime configuration and is intentionally ignored by Git.

Most setups only need:

- `WEMATICS_API_KEY`
- `DATABASE_URL`
- `ARCHIVE_BASE_PATH`
- `FTP_HOST`
- `FTP_USER`
- `FTP_PASSWORD`

The sample file also includes retry, scheduler, alert, and concurrency settings that map directly to `backend/app/core/config.py`.

## Local Development

### Backend

```powershell
cd backend
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Backend API: `http://localhost:8000/api`

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

Frontend dev server: `http://localhost:5173`

The frontend talks to `VITE_API_BASE_URL`, which defaults to `http://localhost:8000/api` in development.

## Available Frontend Scripts

From `frontend/`:

```powershell
npm run dev
npm run build
npm run preview
```

## Backend Tests

From `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

The backend test suite covers:

- timestamp parsing
- diff and gap calculations
- CSV refresh and CSV analysis caching
- FTP path safety and additive upload behavior
- transfer temp workspace cleanup
- job cancellation, resume, overlap guards, and failure recovery
- local inventory scanning behavior

## Docker Compose

```powershell
docker compose up --build
```

Services:

- `frontend`: Nginx serving the Vite build on `http://localhost:5173`
- `backend`: FastAPI on `http://localhost:8000`
- `postgres`: optional Postgres 16 profile on `http://localhost:5432`

Use the Postgres profile with:

```powershell
docker compose --profile postgres up --build
```

The Postgres profile only starts the database container. To make the backend use it, set `DATABASE_URL` in `.env` to a Postgres connection string before starting Compose.

If you use the Postgres profile, set `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` in `.env` instead of relying on the fallback defaults.

## Deployment Notes

- The frontend container builds the Vite app and serves it through Nginx.
- Nginx proxies `/api/` to the backend container.
- The backend container runs `uvicorn app.main:app`.
- SQL migrations in `backend/migrations/*.sql` are applied automatically on startup.
- The Compose setup persists backend database state in `backend_data` and optional Postgres data in `postgres_data`.

## Main API Areas

- `/api/remote/*`
- `/api/local/*`
- `/api/ftp/*`
- `/api/diff/*`
- `/api/jobs/*`
- `/api/schedules/*`
- `/api/glossary`
- `/api/logs`
- `/api/audit`
- `/api/overview`

## Notes On Validation

- Frontend lint and typecheck scripts are not configured in `frontend/package.json`.
- Backend lint and static typecheck tooling are not configured in `backend/`.
- Build and test validation are available through `npm run build` and `pytest`.
