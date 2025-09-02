# Healthcare Pricing API

A FastAPI application with async SQLAlchemy and PostgreSQL for managing healthcare provider pricing data.

## Features

- **FastAPI Backend**: Modern, fast web framework for building APIs
- **Async SQLAlchemy**: High-performance async database operations
- **PostgreSQL Database**: Robust relational database
- **Alembic Migrations**: Database schema management
- **ETL Pipeline**: Data import and processing from CSV files
- **Provider Ratings**: Mock star rating system (1-10)

## Project Structure

```
├── alembic.ini              # Alembic configuration
├── migrations/               # Migration scripts
│   ├── env.py               # Migration environment (async SQLAlchemy ready)
│   ├── versions/            # Generated migration files
│   └── script.py.mako       # Migration template
├── models/                   # SQLAlchemy models
│   └── __init__.py          # Database models with relationships
├── database_config.py        # Database configuration helper
├── etl.py                   # ETL script for data import
├── app.py                   # FastAPI application (to be implemented)
├── requirements.txt          # Python dependencies
└── README.md                # This file
```

## Database Models

### Provider
- `provider_id` (String, Primary Key)
- `provider_name`, `provider_city`, `provider_state`, `provider_zip_code`

### Procedure
- `id` (Integer, Auto-increment Primary Key)
- `ms_drg_code` (String) - CMS DRG code
- `ms_drg_description` (String) - Procedure description

### ProviderProcedure
- Junction table linking providers and procedures
- Financial data: discharges, charges, payments
- Foreign keys to Provider and Procedure

### Rating
- Provider star ratings (1-10 scale)
- Foreign key to Provider

## Setup Instructions

### Prerequisites

1. **Python 3.8+** with pip
2. **PostgreSQL** database (or Docker for containerized setup)
3. **Git** for version control

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd api-assignment
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Database Setup

#### Option 1: Docker (Recommended)

1. **Start PostgreSQL container**
   ```bash
   docker-compose up -d db
   ```

2. **Wait for database to be ready** (check logs: `docker-compose logs -f db`)

#### Option 2: Local PostgreSQL

1. Install PostgreSQL locally
2. Create database: `createdb healthcare`
3. Update `database_config.py` with your connection details

### Database Configuration

Create a `.env` file in your project root:

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

**Note**: The `migrations/env.py` automatically converts async URLs to sync URLs for Alembic.

## Database Migrations

### Initial Setup

1. **Generate initial migration**
   ```bash
   alembic revision --autogenerate -m "init schema"
   ```

2. **Apply migration to create tables**
   ```bash
   alembic upgrade head
   ```

### Migration Commands

- **Check current status**: `alembic current`
- **View migration history**: `alembic history`
- **Rollback to previous version**: `alembic downgrade -1`
- **Rollback to specific version**: `alembic downgrade <revision_id>`

## ETL Data Import

### Import CSV Data

The ETL script processes healthcare pricing data from CSV files:

```bash
python etl.py
```

**Features:**
- Data cleaning and validation
- Mock star rating generation (1-10)
- Async database operations
- Duplicate prevention
- Batch processing for large files

**Data Requirements:**
CSV must contain these columns:
- `provider_id`, `provider_name`, `provider_city`, `provider_state`, `provider_zip_code`
- `ms_drg_definition` (format: "001 - EXCISION OF BRAIN LESION")
- `total_discharges`, `average_covered_charges`, `average_total_payments`, `average_medicare_payments`

### ETL Configuration

- **CSV Path**: Update in `etl.py` main function
- **Batch Size**: Adjust in `HealthcareDataETL` constructor
- **Logging**: Check `etl.log` for detailed processing information

## Running the Application

### Development Server

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### Production

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Verify PostgreSQL is running
   - Check database credentials
   - Ensure database 'healthcare' exists

2. **Models Not Detected**
   - Verify `target_metadata = Base.metadata` in `migrations/env.py`
   - Check that all models are imported in `models/__init__.py`

3. **Permission Errors**
   - Ensure PostgreSQL user has CREATE, ALTER, DROP permissions

4. **Async Driver Error (MissingGreenlet)**
   - **FIXED**: `migrations/env.py` automatically converts async URLs to sync URLs
   - Alembic runs synchronously and cannot use `postgresql+asyncpg://` drivers

5. **ETL Import Errors**
   - Check CSV file format and column names
   - Verify database tables exist (run migrations first)
   - Check `etl.log` for detailed error information

### Database Setup Issues

If using Docker:
```bash
# Check container status
docker-compose ps

# View logs
docker-compose logs -f db

# Restart database
docker-compose restart db

# Clean restart (removes data)
docker-compose down -v
docker-compose up -d db
```

## Development

### Adding New Models

1. Add model class to `models/__init__.py`
2. Generate migration: `alembic revision --autogenerate -m "add new model"`
3. Apply migration: `alembic upgrade head`

### Database Schema Changes

1. Modify models in `models/__init__.py`
2. Generate migration: `alembic revision --autogenerate -m "description"`
3. Review generated migration file
4. Apply: `alembic upgrade head`

## API Endpoints

*To be implemented in `app.py`*

- `GET /providers` - List all providers
- `GET /providers/{provider_id}` - Get provider details
- `GET /procedures` - List all procedures
- `GET /providers/{provider_id}/procedures` - Get provider procedures
- `GET /ratings` - List provider ratings

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Commit changes: `git commit -am 'Add feature'`
4. Push branch: `git push origin feature-name`
5. Submit pull request

## License

[Add your license information here]
