#!/usr/bin/env python3
"""
ETL Script for Healthcare Pricing Data

This script reads CSV data from sample_prices_ny.csv, filters for NY providers,
cleans it, generates mock ratings, and loads it into PostgreSQL tables using 
async SQLAlchemy. It can be run multiple times without duplicating records.

Features:
- NY state filtering (Rndrng_Prvdr_State_Abrvtn == 'NY')
- Data cleaning and validation
- Mock star rating generation (1-10)
- Async database operations
- Duplicate prevention
- Comprehensive error handling
- Transaction management
"""

import asyncio
import csv
import logging
import os
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from models import Base, Provider, Procedure, ProviderProcedure, Rating
from database_config import get_database_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HealthcareDataETL:
    """ETL processor for healthcare pricing data"""
    
    def __init__(self, csv_path: str, batch_size: int = 1000):
        """
        Initialize ETL processor
        
        Args:
            csv_path: Path to the CSV file
            batch_size: Number of records to process in each batch
        """
        self.csv_path = csv_path
        self.batch_size = batch_size
        self.engine = None
        self.session_factory = None
        
    async def initialize_database(self):
        """Initialize database connection and session factory"""
        try:
            database_url = get_database_url(async_driver=True)
            self.engine = create_async_engine(
                database_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=300
            )
            
            # Test connection
            async with self.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            
            self.session_factory = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def clean_text_field(self, text: str, max_length: int = None) -> str:
        """
        Clean text field by removing problematic characters and handling encoding issues
        
        Args:
            text: Raw text to clean
            max_length: Maximum length to truncate to
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Convert to string and strip whitespace
        text = str(text).strip()
        
        # Remove any non-printable characters that might cause encoding issues
        text = ''.join(char for char in text if char.isprintable() or char.isspace())
        
        # Clean up extra whitespace
        text = ' '.join(text.split())
        
        # Truncate if max_length specified
        if max_length and len(text) > max_length:
            text = text[:max_length]
        
        return text
    
    def log_problematic_row_details(self, chunk, batch_count: int, error: Exception):
        """
        Log detailed information about a problematic row
        
        Args:
            chunk: The pandas DataFrame chunk
            batch_count: Current batch number
            error: The exception that occurred
        """
        try:
            logger.error(f"=== PROBLEMATIC ROW ANALYSIS ===")
            logger.error(f"Batch: {batch_count}")
            logger.error(f"Error: {error}")
            logger.error(f"Error type: {type(error).__name__}")
            
            if hasattr(error, 'start') and hasattr(error, 'end'):
                logger.error(f"Error position: {error.start}-{error.end}")
            
            # Try to get row information
            if chunk is not None and hasattr(chunk, 'iloc'):
                logger.error(f"Chunk size: {len(chunk)}")
                logger.error(f"Chunk columns: {list(chunk.columns)}")
                
                # Try to identify which row might be problematic
                try:
                    # Show first few rows for context
                    logger.error("First 3 rows of chunk:")
                    for i in range(min(3, len(chunk))):
                        row_data = chunk.iloc[i].to_dict()
                        # Clean the row data for logging
                        clean_row = {}
                        for key, value in row_data.items():
                            try:
                                clean_value = str(value)[:100]  # Limit length
                                clean_row[key] = clean_value
                            except Exception:
                                clean_row[key] = f"<unprintable: {type(value)}>"
                        logger.error(f"  Row {i}: {clean_row}")
                        
                except Exception as row_error:
                    logger.error(f"Could not extract row details: {row_error}")
            
            logger.error(f"=== END PROBLEMATIC ROW ANALYSIS ===")
            
        except Exception as log_error:
            logger.error(f"Error in logging problematic row details: {log_error}")
    
    def clean_provider_data(self, row: Dict) -> Optional[Dict]:
        """
        Clean and validate provider data from CSV row with new column names
        
        Args:
            row: Raw CSV row data
            
        Returns:
            Cleaned provider data or None if invalid
        """
        try:
            # Extract and clean provider fields using new CSV column names
            provider_id = self.clean_text_field(row.get('Rndrng_Prvdr_CCN', ''))
            provider_name = self.clean_text_field(row.get('Rndrng_Prvdr_Org_Name', ''), 255)
            provider_city = self.clean_text_field(row.get('Rndrng_Prvdr_City', ''), 100)
            provider_state = self.clean_text_field(row.get('Rndrng_Prvdr_State_Abrvtn', ''))
            provider_zip_code = self.clean_text_field(row.get('Rndrng_Prvdr_Zip5', ''))
            
            # Validate required fields
            if not all([provider_id, provider_name, provider_city, provider_state, provider_zip_code]):
                logger.warning(f"Skipping row with missing provider data: {provider_id}")
                return None
            
            # Clean zip code (remove non-numeric characters, ensure 5 digits)
            provider_zip_code = ''.join(filter(str.isdigit, provider_zip_code))
            if len(provider_zip_code) < 5:
                provider_zip_code = provider_zip_code.zfill(5)
            elif len(provider_zip_code) > 5:
                provider_zip_code = provider_zip_code[:5]
            
            # Clean state (ensure 2-letter format)
            provider_state = provider_state.upper()[:2]
            
            return {
                'provider_id': provider_id,
                'provider_name': provider_name,
                'provider_city': provider_city.title(),
                'provider_state': provider_state,
                'provider_zip_code': provider_zip_code
            }
            
        except Exception as e:
            logger.error(f"Error cleaning provider data: {e}")
            return None
    
    def clean_procedure_data(self, row: Dict) -> Optional[Dict]:
        """
        Clean and validate procedure data from CSV row with new column names
        
        Args:
            row: Raw CSV row data
            
        Returns:
            Cleaned procedure data or None if invalid
        """
        try:
            # Extract DRG code and description using new CSV column names
            ms_drg_code = self.clean_text_field(row.get('DRG_Cd', ''))
            ms_drg_description = self.clean_text_field(row.get('DRG_Desc', ''), 500)
            
            if not ms_drg_code or not ms_drg_description:
                logger.warning(f"Skipping row with missing procedure data: {ms_drg_code}")
                return None
            
            # Clean DRG code (remove any non-alphanumeric characters)
            ms_drg_code = ''.join(c for c in ms_drg_code if c.isalnum())
            
            return {
                'ms_drg_code': ms_drg_code,
                'ms_drg_description': ms_drg_description
            }
            
        except Exception as e:
            logger.error(f"Error cleaning procedure data: {e}")
            return None
    
    def clean_financial_data(self, row: Dict) -> Optional[Dict]:
        """
        Clean and validate financial data from CSV row with new column names
        
        Args:
            row: Raw CSV row data
            
        Returns:
            Cleaned financial data or None if invalid
        """
        try:
            # Extract numeric fields using new CSV column names
            total_discharges = row.get('Tot_Dschrgs')
            average_covered_charges = row.get('Avg_Submtd_Cvrd_Chrg')
            average_total_payments = row.get('Avg_Tot_Pymt_Amt')
            average_medicare_payments = row.get('Avg_Mdcr_Pymt_Amt')
            
            # Convert to numeric values, handling various formats
            def clean_numeric(value):
                if pd.isna(value) or value == '':
                    return None
                try:
                    # Remove currency symbols and commas
                    if isinstance(value, str):
                        value = value.replace('$', '').replace(',', '').strip()
                    return float(value) if value else None
                except (ValueError, TypeError):
                    return None
            
            total_discharges = clean_numeric(total_discharges)
            average_covered_charges = clean_numeric(average_covered_charges)
            average_total_payments = clean_numeric(average_total_payments)
            average_medicare_payments = clean_numeric(average_medicare_payments)
            
            # Validate that we have at least some financial data
            if all(v is None for v in [total_discharges, average_covered_charges, 
                                      average_total_payments, average_medicare_payments]):
                logger.warning("Skipping row with no valid financial data")
                return None
            
            return {
                'total_discharges': total_discharges,
                'average_covered_charges': average_covered_charges,
                'average_total_payments': average_total_payments,
                'average_medicare_payments': average_medicare_payments
            }
            
        except Exception as e:
            logger.error(f"Error cleaning financial data: {e}")
            return None
    
    def filter_ny_providers(self, batch_data: List[Dict]) -> List[Dict]:
        """
        Filter batch data to include only NY providers
        
        Args:
            batch_data: List of CSV row dictionaries
            
        Returns:
            Filtered list containing only NY providers
        """
        try:
            ny_providers = []
            for row in batch_data:
                state = str(row.get('Rndrng_Prvdr_State_Abrvtn', '')).strip().upper()
                if state == 'NY':
                    ny_providers.append(row)
            
            return ny_providers
            
        except Exception as e:
            logger.error(f"Error filtering NY providers: {e}")
            return []
    
    def generate_mock_rating(self, provider_id: str) -> int:
        """
        Generate a mock star rating (1-10) for a provider
        
        Args:
            provider_id: Provider identifier for consistent rating generation
            
        Returns:
            Mock rating between 1 and 10
        """
        # Use provider_id hash for consistent rating generation
        random.seed(hash(provider_id) % 2**32)
        rating = random.randint(1, 10)
        random.seed()  # Reset seed
        return rating
    
    async def get_or_create_provider(self, session: AsyncSession, provider_data: Dict) -> Provider:
        """
        Get existing provider or create new one
        
        Args:
            session: Database session
            provider_data: Cleaned provider data
            
        Returns:
            Provider instance
        """
        # Check if provider exists
        stmt = select(Provider).where(Provider.provider_id == provider_data['provider_id'])
        result = await session.execute(stmt)
        existing_provider = result.scalar_one_or_none()
        
        if existing_provider:
            # Update existing provider data
            for key, value in provider_data.items():
                setattr(existing_provider, key, value)
            logger.debug(f"Updated existing provider: {provider_data['provider_id']}")
            return existing_provider
        else:
            # Create new provider
            new_provider = Provider(**provider_data)
            session.add(new_provider)
            logger.debug(f"Created new provider: {provider_data['provider_id']}")
            return new_provider
    
    async def get_or_create_procedure(self, session: AsyncSession, procedure_data: Dict) -> Procedure:
        """
        Get existing procedure or create new one
        
        Args:
            session: Database session
            procedure_data: Cleaned procedure data
            
        Returns:
            Procedure instance
        """
        # Check if procedure exists
        stmt = select(Procedure).where(Procedure.ms_drg_code == procedure_data['ms_drg_code'])
        result = await session.execute(stmt)
        existing_procedure = result.scalar_one_or_none()
        
        if existing_procedure:
            # Update existing procedure description
            existing_procedure.ms_drg_description = procedure_data['ms_drg_description']
            logger.debug(f"Updated existing procedure: {procedure_data['ms_drg_code']}")
            return existing_procedure
        else:
            # Create new procedure
            new_procedure = Procedure(**procedure_data)
            session.add(new_procedure)
            logger.debug(f"Created new procedure: {procedure_data['ms_drg_code']}")
            return new_procedure
    
    async def upsert_provider_procedure(self, session: AsyncSession, 
                                      provider: Provider, procedure: Procedure, 
                                      financial_data: Dict) -> ProviderProcedure:
        """
        Upsert provider-procedure relationship with financial data
        
        Args:
            session: Database session
            provider: Provider instance
            procedure: Procedure instance
            financial_data: Cleaned financial data
            
        Returns:
            ProviderProcedure instance
        """
        # Check if relationship exists
        stmt = select(ProviderProcedure).where(
            ProviderProcedure.provider_id == provider.provider_id,
            ProviderProcedure.procedure_id == procedure.id
        )
        result = await session.execute(stmt)
        existing_pp = result.scalar_one_or_none()
        
        if existing_pp:
            # Update existing relationship
            for key, value in financial_data.items():
                if value is not None:  # Only update non-None values
                    setattr(existing_pp, key, value)
            logger.debug(f"Updated existing provider-procedure relationship")
            return existing_pp
        else:
            # Create new relationship
            new_pp = ProviderProcedure(
                provider_id=provider.provider_id,
                procedure_id=procedure.id,
                **financial_data
            )
            session.add(new_pp)
            logger.debug(f"Created new provider-procedure relationship")
            return new_pp
    
    async def upsert_rating(self, session: AsyncSession, provider: Provider, rating_value: int) -> Rating:
        """
        Upsert provider rating
        
        Args:
            session: Database session
            provider: Provider instance
            rating_value: Rating value (1-10)
            
        Returns:
            Rating instance
        """
        # Check if rating exists
        stmt = select(Rating).where(Rating.provider_id == provider.provider_id)
        result = await session.execute(stmt)
        existing_rating = result.scalar_one_or_none()
        
        if existing_rating:
            # Update existing rating
            existing_rating.rating = rating_value
            logger.debug(f"Updated existing rating for provider: {provider.provider_id}")
            return existing_rating
        else:
            # Create new rating
            new_rating = Rating(
                provider_id=provider.provider_id,
                rating=rating_value
            )
            session.add(new_rating)
            logger.debug(f"Created new rating for provider: {provider.provider_id}")
            return new_rating
    
    async def process_batch(self, session: AsyncSession, batch_data: List[Dict]) -> Tuple[int, int]:
        """
        Process a batch of CSV rows
        
        Args:
            session: Database session
            batch_data: List of CSV row dictionaries
            
        Returns:
            Tuple of (processed_count, error_count)
        """
        processed_count = 0
        error_count = 0
        
        for row in batch_data:
            try:
                # Clean data
                provider_data = self.clean_provider_data(row)
                procedure_data = self.clean_procedure_data(row)
                financial_data = self.clean_financial_data(row)
                
                if not all([provider_data, procedure_data, financial_data]):
                    error_count += 1
                    continue
                
                # Get or create provider and procedure
                provider = await self.get_or_create_provider(session, provider_data)
                procedure = await self.get_or_create_procedure(session, procedure_data)
                
                # Upsert provider-procedure relationship
                await self.upsert_provider_procedure(session, provider, procedure, financial_data)
                
                # Generate and upsert rating (only once per provider)
                if not hasattr(self, '_rated_providers'):
                    self._rated_providers = set()
                
                if provider.provider_id not in self._rated_providers:
                    rating_value = self.generate_mock_rating(provider.provider_id)
                    await self.upsert_rating(session, provider, rating_value)
                    self._rated_providers.add(provider.provider_id)
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                error_count += 1
                continue
        
        return processed_count, error_count
    
    async def run_etl(self):
        """Run the complete ETL process"""
        try:
            logger.info("Starting ETL process...")
            
            # Initialize database
            await self.initialize_database()
            
            # Read CSV file
            logger.info(f"Reading CSV file: {self.csv_path}")
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
            
            # Process CSV in batches
            total_processed = 0
            total_errors = 0
            batch_count = 0
            
            async with self.session_factory() as session:
                async with session.begin():
                    # Read CSV in chunks to handle large files
                    # Try different encodings to handle various file formats
                    encodings_to_try = ['latin-1', 'cp1252', 'iso-8859-1', 'utf-8']
                    chunk_iter = None
                    
                    for encoding in encodings_to_try:
                        try:
                            # Test with a small chunk first
                            test_chunk = pd.read_csv(
                                self.csv_path,
                                nrows=10,
                                low_memory=False,
                                dtype=str,
                                encoding=encoding,
                                on_bad_lines='skip'  # Skip problematic lines
                            )
                            logger.info(f"Successfully tested CSV with encoding: {encoding}")
                            
                            # If test successful, create the full chunk iterator
                            chunk_iter = pd.read_csv(
                                self.csv_path,
                                chunksize=self.batch_size,
                                low_memory=False,
                                dtype=str,
                                encoding=encoding,
                                on_bad_lines='skip'  # Skip problematic lines
                            )
                            break
                        except UnicodeDecodeError as e:
                            logger.warning(f"Failed to read CSV with encoding: {encoding} - {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error testing encoding {encoding}: {e}")
                            continue
                    
                    if chunk_iter is None:
                        # Fallback: try with error handling
                        logger.warning("All encodings failed, trying with error handling...")
                        try:
                            chunk_iter = pd.read_csv(
                                self.csv_path,
                                chunksize=self.batch_size,
                                low_memory=False,
                                dtype=str,
                                encoding='latin-1',  # Most permissive encoding
                                on_bad_lines='skip',
                                encoding_errors='replace'
                            )
                            logger.info("Successfully opened CSV with fallback method")
                        except Exception as e:
                            raise ValueError(f"Could not read CSV file with any method: {e}")
                    
                    for chunk in chunk_iter:
                        batch_count += 1
                        logger.info(f"Processing batch {batch_count} ({len(chunk)} records)")
                        
                        try:
                            # Convert chunk to list of dictionaries
                            batch_data = chunk.to_dict('records')
                            
                            # Filter for NY providers only
                            ny_batch_data = self.filter_ny_providers(batch_data)
                            
                            if not ny_batch_data:
                                logger.info(f"Batch {batch_count}: No NY providers found, skipping")
                                continue
                            
                            logger.info(f"Batch {batch_count}: Filtered to {len(ny_batch_data)} NY providers from {len(batch_data)} total records")
                            
                            # Process batch
                            processed, errors = await self.process_batch(session, ny_batch_data)
                            
                            total_processed += processed
                            total_errors += errors
                            
                            logger.info(f"Batch {batch_count} completed: {processed} processed, {errors} errors")
                            
                        except UnicodeDecodeError as e:
                            logger.error(f"Unicode decode error in batch {batch_count}: {e}")
                            self.log_problematic_row_details(chunk, batch_count, e)
                            
                            # Skip this batch and continue
                            logger.warning(f"Skipping batch {batch_count} due to encoding error")
                            continue
                            
                        except Exception as e:
                            logger.error(f"Unexpected error in batch {batch_count}: {e}")
                            self.log_problematic_row_details(chunk, batch_count, e)
                            
                            # Skip this batch and continue
                            logger.warning(f"Skipping batch {batch_count} due to error")
                            continue
                    
                    # Commit all changes
                    await session.commit()
            
            logger.info(f"ETL process completed successfully!")
            logger.info(f"Total records processed: {total_processed}")
            logger.info(f"Total errors: {total_errors}")
            logger.info(f"Total batches: {batch_count}")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        
        finally:
            if self.engine:
                await self.engine.dispose()


async def main():
    """Main function to run ETL process"""
    try:
        # Initialize ETL processor
        etl = HealthcareDataETL(
            csv_path="data/sample_prices_ny.csv",
            batch_size=1000
        )
        
        # Run ETL process
        await etl.run_etl()
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise


if __name__ == "__main__":
    # Run ETL process
    asyncio.run(main())
