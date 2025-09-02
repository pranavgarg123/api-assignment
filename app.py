#!/usr/bin/env python3
"""
FastAPI Healthcare Pricing API

This API provides endpoints for searching healthcare providers and an AI assistant
for natural language queries about healthcare pricing and quality data.

Endpoints:
- GET /providers: Search providers by DRG, ZIP code, and radius
- POST /ask: AI assistant for natural language queries
"""

import asyncio
import json
import logging
import os
from typing import List, Optional, Dict, Any
import math

from openai import OpenAI
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import selectinload

from models import Base, Provider, Procedure, ProviderProcedure, Rating
from database_config import get_database_url

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Healthcare Pricing API",
    description="API for searching healthcare providers and AI-powered queries",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
engine = None
session_factory = None

# OpenAI setup
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Pydantic schemas
class ProviderSearchRequest(BaseModel):
    drg: Optional[str] = Field(None, description="MS-DRG code or description")
    zip: Optional[str] = Field(None, description="ZIP code for location search")
    radius_km: Optional[float] = Field(10.0, description="Search radius in kilometers")

class ProviderResponse(BaseModel):
    provider_id: str
    provider_name: str
    provider_city: str
    provider_state: str
    provider_zip_code: str
    distance_km: Optional[float] = None
    ms_drg_code: str
    ms_drg_description: str
    total_discharges: int
    average_covered_charges: float
    average_total_payments: float
    average_medicare_payments: float
    rating: Optional[int] = None

class AskRequest(BaseModel):
    question: str = Field(..., description="Natural language question about healthcare data")

class AskResponse(BaseModel):
    question: str
    sql_query: str
    results: List[Dict[str, Any]]
    message: str

# Database dependency
async def get_db():
    """Get database session"""
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.close()

# Helper functions
def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = (math.sin(dlat/2) * math.sin(dlat/2) + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(dlon/2) * math.sin(dlon/2))
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

async def get_zip_coordinates(zip_code: str) -> Optional[tuple]:
    """Get coordinates for a ZIP code (simplified - in production, use a proper geocoding service)"""
    # This is a simplified implementation
    # In production, you'd use a service like Google Geocoding API or a ZIP code database
    zip_coords = {
        "10001": (40.7505, -73.9934),  # NYC
        "10002": (40.7174, -73.9897),  # NYC
        "10003": (40.7323, -73.9894),  # NYC
        "11201": (40.6943, -73.9903),  # Brooklyn
        "11215": (40.6622, -73.9874),  # Brooklyn
        "10451": (40.8200, -73.9200),  # Bronx
        "11101": (40.7505, -73.9400),  # Queens
        "10301": (40.6415, -74.0776),  # Staten Island
    }
    return zip_coords.get(zip_code)

async def generate_sql_from_question(question: str) -> str:
    """Use OpenAI to convert natural language question to SQL query"""
    system_prompt = """
        You are a SQL expert for a healthcare pricing database. Convert natural language questions into SQL queries.
        
        Database schema:
        - providers: provider_id, provider_name, provider_city, provider_state, provider_zip_code
        - procedures: id, ms_drg_code, ms_drg_description  
        - provider_procedures: provider_id, procedure_id, total_discharges, average_covered_charges, average_total_payments, average_medicare_payments
        - ratings: provider_id, rating (1-10 scale)
        
        Rules:
        1. Always use JOINs to get complete data
        2. Use ILIKE for text searches
        3. Return only the SQL query, no explanations
        4. Use proper table aliases (p for providers, pr for procedures, pp for provider_procedures, r for ratings)
        5. For cost queries, use average_total_payments
        6. For quality queries, use ratings.rating
        7. Always include provider_name and relevant procedure info in SELECT
        """
        
    # Use asyncio.to_thread to call synchronous client in async function

    await asyncio.sleep(2)

    response = await asyncio.to_thread(
    lambda: client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ],
        max_tokens=500,
        temperature=0.1
    )
    )

    return response.choices[0].message.content.strip()

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database connection"""
    global engine, session_factory
    
    try:
        database_url = get_database_url(async_driver=True)
        engine = create_async_engine(
            database_url,
            echo=False,
            pool_pre_ping=True,
            pool_recycle=300
        )
        
        session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("Database connection established successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection"""
    if engine:
        await engine.dispose()
        logger.info("Database connection closed")

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Healthcare Pricing API",
        "version": "1.0.0",
        "endpoints": {
            "GET /providers": "Search providers by DRG, ZIP, and radius",
            "POST /ask": "AI assistant for natural language queries"
        }
    }

@app.get("/providers", response_model=List[ProviderResponse])
async def search_providers(
    drg: Optional[str] = Query(None, description="MS-DRG code or description"),
    zip: Optional[str] = Query(None, description="ZIP code for location search"),
    radius_km: Optional[float] = Query(10.0, description="Search radius in kilometers"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search for healthcare providers offering specific DRG procedures within a radius of a ZIP code.
    
    - **drg**: MS-DRG code (e.g., "001") or description (e.g., "heart surgery")
    - **zip**: ZIP code for location-based search
    - **radius_km**: Search radius in kilometers (default: 10.0)
    """
    try:
        # Build base query
        query = select(
            Provider.provider_id,
            Provider.provider_name,
            Provider.provider_city,
            Provider.provider_state,
            Provider.provider_zip_code,
            Procedure.ms_drg_code,
            Procedure.ms_drg_description,
            ProviderProcedure.total_discharges,
            ProviderProcedure.average_covered_charges,
            ProviderProcedure.average_total_payments,
            ProviderProcedure.average_medicare_payments,
            Rating.rating
        ).join(
            ProviderProcedure, Provider.provider_id == ProviderProcedure.provider_id
        ).join(
            Procedure, ProviderProcedure.procedure_id == Procedure.id
        ).outerjoin(
            Rating, Provider.provider_id == Rating.provider_id
        )
        
        # Add DRG filter if provided
        if drg:
            # Try to match by code first, then by description
            if drg.isdigit():
                query = query.where(Procedure.ms_drg_code == drg)
            else:
                query = query.where(Procedure.ms_drg_description.ilike(f"%{drg}%"))
        
        # Execute query
        result = await db.execute(query)
        rows = result.fetchall()
        
        # Convert to list of dictionaries
        providers = []
        for row in rows:
            provider_data = {
                "provider_id": row.provider_id,
                "provider_name": row.provider_name,
                "provider_city": row.provider_city,
                "provider_state": row.provider_state,
                "provider_zip_code": row.provider_zip_code,
                "ms_drg_code": row.ms_drg_code,
                "ms_drg_description": row.ms_drg_description,
                "total_discharges": row.total_discharges,
                "average_covered_charges": row.average_covered_charges,
                "average_total_payments": row.average_total_payments,
                "average_medicare_payments": row.average_medicare_payments,
                "rating": row.rating
            }
            providers.append(provider_data)
        
        # Filter by location if ZIP code provided
        if zip and radius_km:
            target_coords = await get_zip_coordinates(zip)
            if target_coords:
                filtered_providers = []
                for provider in providers:
                    provider_coords = await get_zip_coordinates(provider["provider_zip_code"])
                    if provider_coords:
                        distance = calculate_distance(
                            target_coords[0], target_coords[1],
                            provider_coords[0], provider_coords[1]
                        )
                        if distance <= radius_km:
                            provider["distance_km"] = round(distance, 2)
                            filtered_providers.append(provider)
                providers = filtered_providers
            else:
                logger.warning(f"Could not find coordinates for ZIP code: {zip}")
        
        return providers
        
    except Exception as e:
        logger.error(f"Error searching providers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/ask", response_model=AskResponse)
async def ask_question(
    request: AskRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    AI assistant endpoint that converts natural language questions to SQL queries
    and returns results from the healthcare database.
    
    Supports queries about:
    - Cost comparisons (cheapest hospitals for procedures)
    - Quality ratings (highest rated hospitals)
    - Provider information and statistics
    - Procedure details and pricing
    """
    try:
        # Generate SQL query from natural language question
        sql_query = await generate_sql_from_question(request.question)
        
        # Validate that the query is safe (basic validation)
        dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE"]
        if any(keyword in sql_query.upper() for keyword in dangerous_keywords):
            return AskResponse(
                question=request.question,
                sql_query="",
                results=[],
                message="Sorry, I can only answer read-only questions about healthcare data."
            )
        
        # Execute the SQL query
        try:
            result = await db.execute(sql_query)
            rows = result.fetchall()
            
            # Convert results to list of dictionaries
            results = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(result.keys()):
                    value = row[i]
                    # Convert non-serializable types
                    if hasattr(value, 'isoformat'):  # datetime
                        value = value.isoformat()
                    row_dict[column] = value
                results.append(row_dict)
            
            message = f"Found {len(results)} results for your query."
            
        except Exception as sql_error:
            logger.error(f"SQL execution error: {sql_error}")
            return AskResponse(
                question=request.question,
                sql_query=sql_query,
                results=[],
                message="Sorry, I couldn't execute that query. Please try rephrasing your question."
            )
        
        return AskResponse(
            question=request.question,
            sql_query=sql_query,
            results=results,
            message=message
        )
        
    except Exception as e:
        logger.error(f"Error processing question: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
