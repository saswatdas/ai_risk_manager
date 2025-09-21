# backend/main.py (FastAPI Backend)
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import os
from dotenv import load_dotenv
import logging
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Project Risk API", version="1.0.0")

# CORS middleware to allow Streamlit frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development, restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'mydb'),
    'user': os.getenv('DB_USER', 'myuser'),
    'password': os.getenv('DB_PASSWORD', 'password@123'),
    'port': os.getenv('DB_PORT', '5432'),
    'cursor_factory': RealDictCursor
}

# Pydantic models for request/response
class RiskAssessment(BaseModel):
    id: Optional[int] = None
    project_id: str
    project_name: str
    rating_date: date
    optic_name: str
    rating: str
    justification: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class ProjectInfo(BaseModel):
    project_id: str
    project_name: str
    latest_assessment_date: Optional[date] = None
    total_assessments: Optional[int] = 0

class ProjectTrend(BaseModel):
    rating_date: date
    optic_name: str
    rating: str

def get_db_connection():
    """Create and return a database connection."""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/")
async def root():
    return {"message": "Project Risk API is running"}

@app.get("/api/projects", response_model=List[ProjectInfo])
async def get_all_projects():
    """Get list of all projects with their latest assessment date."""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = """
            SELECT 
                project_id,
                project_name,
                MAX(rating_date) as latest_assessment_date,
                COUNT(*) as total_assessments
            FROM risk_assessments 
            GROUP BY project_id, project_name
            ORDER BY project_name
            """
            cursor.execute(query)
            projects = cursor.fetchall()
            return [ProjectInfo(**project) for project in projects]
    except Exception as e:
        logger.error(f"Error fetching projects: {e}")
        raise HTTPException(status_code=500, detail="Error fetching projects")
    finally:
        if connection:
            connection.close()

@app.get("/api/projects/{project_id}/assessments", response_model=List[RiskAssessment])
async def get_project_assessments(project_id: str, limit: int = 100):
    """Get all risk assessments for a specific project."""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = """
            SELECT * FROM risk_assessments 
            WHERE project_id = %s 
            ORDER BY rating_date DESC, optic_name
            LIMIT %s
            """
            cursor.execute(query, (project_id, limit))
            assessments = cursor.fetchall()
            
            if not assessments:
                raise HTTPException(status_code=404, detail="Project not found")
            
            return [RiskAssessment(**assessment) for assessment in assessments]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching assessments for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching assessments")
    finally:
        if connection:
            connection.close()

@app.get("/api/projects/{project_id}/latest", response_model=List[RiskAssessment])
async def get_latest_assessments(project_id: str):
    """Get the latest risk assessments for a project."""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = """
            SELECT DISTINCT ON (optic_name) *
            FROM risk_assessments 
            WHERE project_id = %s 
            ORDER BY optic_name, rating_date DESC
            """
            cursor.execute(query, (project_id,))
            assessments = cursor.fetchall()
            
            if not assessments:
                raise HTTPException(status_code=404, detail="Project not found")
            
            return [RiskAssessment(**assessment) for assessment in assessments]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest assessments for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest assessments")
    finally:
        if connection:
            connection.close()

@app.get("/api/projects/{project_id}/trends", response_model=List[ProjectTrend])
async def get_project_trends(project_id: str, optic_name: Optional[str] = None):
    """Get trend data for a project (all assessments over time)."""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            if optic_name:
                query = """
                SELECT rating_date, optic_name, rating
                FROM risk_assessments 
                WHERE project_id = %s AND optic_name = %s
                ORDER BY rating_date, optic_name
                """
                cursor.execute(query, (project_id, optic_name))
            else:
                query = """
                SELECT rating_date, optic_name, rating
                FROM risk_assessments 
                WHERE project_id = %s
                ORDER BY rating_date, optic_name
                """
                cursor.execute(query, (project_id,))
            
            trends = cursor.fetchall()
            return [ProjectTrend(**trend) for trend in trends]
    except Exception as e:
        logger.error(f"Error fetching trends for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching trends")
    finally:
        if connection:
            connection.close()

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )