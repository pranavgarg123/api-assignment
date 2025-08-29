# Alembic Migration Setup Guide

This guide explains how to set up and run database migrations for your FastAPI app with async SQLAlchemy and PostgreSQL.

## Prerequisites

1. **PostgreSQL Database**: Make sure your PostgreSQL database is running (you can use the provided `docker-compose.yml`)
2. **Environment Variables**: Set up your database connection details

## Database Configuration

### Option 1: Environment Variables (Recommended)

Create a `.env` file in your project root with:

```bash
# Database URL for your FastAPI app (async)
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/healthcare

# Or use individual parameters
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=healthcare
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

### Option 2: Use the Configuration File

The `database_config.py` file contains default values that you can modify directly.

## Running Migrations

### 1. Generate Initial Migration

```bash
alembic revision --autogenerate -m "init schema"
```

This will:
- Detect your SQLAlchemy models from `models/__init__.py`
- Generate a migration file in `migrations/versions/`
- Include all tables: `providers`, `procedures`, `provider_procedures`, `ratings`

### 2. Apply the Migration

```bash
alembic upgrade head
```

This will:
- Create all the database tables
- Set up foreign key relationships
- Create indexes

## Migration Commands

### Check Current Status
```bash
alembic current
```

### View Migration History
```bash
alembic history
```

### Rollback to Previous Version
```bash
alembic downgrade -1
```

### Rollback to Specific Version
```bash
alembic downgrade <revision_id>
```

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Verify PostgreSQL is running
   - Check your database credentials
   - Ensure the database exists

2. **Models Not Detected**
   - Make sure `models/__init__.py` imports all your models
   - Verify `target_metadata = Base.metadata` is set in `migrations/env.py`

3. **Permission Errors**
   - Ensure your PostgreSQL user has CREATE, ALTER, DROP permissions

4. **Async Driver Error (MissingGreenlet)**
   - **FIXED**: The `migrations/env.py` now automatically converts async URLs to sync URLs
   - Alembic runs synchronously and cannot use `postgresql+asyncpg://` drivers
   - Your FastAPI app can still use async drivers, but migrations use sync drivers

### Database Setup

If you're using the provided `docker-compose.yml`:

```bash
# Start PostgreSQL
docker-compose up -d db

# Wait for database to be ready, then run migrations
alembic upgrade head
```

## File Structure

```
├── alembic.ini              # Alembic configuration
├── migrations/               # Migration scripts
│   ├── env.py               # Migration environment (updated for async)
│   ├── versions/            # Generated migration files
│   └── script.py.mako       # Migration template
├── models/                   # Your SQLAlchemy models
│   └── __init__.py          # Models with Base.metadata
├── database_config.py        # Database configuration helper
└── MIGRATION_SETUP.md       # This file
```

## Next Steps

After running migrations successfully:

1. Your database tables will be created
2. You can start your FastAPI app
3. The async SQLAlchemy engine will connect to the database
4. Your models will be ready to use in API endpoints

## Notes

- **Alembic runs in sync mode**: Even though your app uses async SQLAlchemy, Alembic migrations run synchronously
- **Environment Variables**: The `DATABASE_URL` environment variable takes precedence over the `alembic.ini` configuration
- **Model Detection**: All models must be imported in `models/__init__.py` for Alembic to detect them
