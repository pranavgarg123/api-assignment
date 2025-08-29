"""
Database Configuration for FastAPI App with Async SQLAlchemy and PostgreSQL

This file contains the database configuration that should be used by your FastAPI app.
Copy the DATABASE_URL to your .env file or set it as an environment variable.

For Alembic migrations, you can use either:
- postgresql:// (sync) - for Alembic
- postgresql+asyncpg:// (async) - for your FastAPI app
"""

import os

# Database URL for async SQLAlchemy (FastAPI app)
ASYNC_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://user:password@localhost:5432/healthcare"
)

# Database URL for sync SQLAlchemy (Alembic migrations)
SYNC_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://user:password@localhost:5432/healthcare"
)

# Individual connection parameters
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "healthcare")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Connection string builder
def get_database_url(async_driver=True):
    """Get database URL with optional async driver"""
    if async_driver:
        return f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    else:
        return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
