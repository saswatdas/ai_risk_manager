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
import asyncpg

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
    'port': os.getenv('DB_PORT', '5432')
}

# Pydantic models for request/response
class RowData(BaseModel):
    project_id: str
    project_name: str
    updated: str
    portfolio_manager: str = ""
    executive_summary: str = ""
    comments_on_schedule: str = ""
    comments_on_budget: str = ""
    comments_on_cost: str = ""
    comments_on_resources: str = ""
    comments_on_scope: str = ""
    comments: str = ""
    key_activities_planned: str = ""
    last_month_achievements: str = ""
    business_value_comment: str = ""
    combined_data: str = ""

class ProcessRequest(BaseModel):
    file_path: str
    rows: List[RowData]
    total_rows: int
    
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
        print("DB_CONFIG::",DB_CONFIG)
        connection = asyncpg.connect(**DB_CONFIG)
        print("database connection.....", connection)
        return connection
    except Exception as e:
        print(f"Database connection error: {e}")
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/")
async def root():
    return {"message": "Project Risk API is running"}

@app.get("/api/projects", response_model=List[ProjectInfo])
async def get_all_projects():
    """Get list of all projects with their latest assessment date."""
    connection = None
    try:
        connection = await get_db_connection()
        query = """
        SELECT 
            project_id,
            project_name,
            MAX(rating_date) AS latest_assessment_date,
            COUNT(*) AS total_assessments
        FROM risk_assessments 
        GROUP BY project_id, project_name
        ORDER BY project_name
        """
        rows = await connection.fetch(query)
        
        return [
            ProjectInfo(
                project_id=row["project_id"],
                project_name=row["project_name"],
                latest_assessment_date=row["latest_assessment_date"],
                total_assessments=row["total_assessments"]
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching projects: {e}")
        raise HTTPException(status_code=500, detail="Error fetching projects")
    finally:
        if connection:
            await connection.close()

@app.get("/api/projects/{project_id}/assessments", response_model=List[RiskAssessment])
async def get_project_assessments(project_id: str, limit: int = 100):
    """Get all risk assessments for a specific project."""
    connection = None
    try:
        connection = await get_db_connection()
        query = """
        SELECT *
        FROM risk_assessments 
        WHERE project_id = $1
        ORDER BY rating_date DESC, optic_name
        LIMIT $2
        """
        rows = await connection.fetch(query, project_id, limit)

        if not rows:
            raise HTTPException(status_code=404, detail="Project not found")

        return [
            RiskAssessment(
                id=row["id"],
                project_id=row["project_id"],
                project_name=row["project_name"],
                rating_date=row["rating_date"],
                optic_name=row["optic_name"],
                rating=row["rating"],
                justification=row["justification"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching assessments for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching assessments")
    finally:
        if connection:
            await connection.close()


@app.get("/api/projects/{project_id}/latest", response_model=List[RiskAssessment])
async def get_latest_assessments(project_id: str):
    """Get the latest risk assessments for a project."""
    connection = None
    try:
        connection = await get_db_connection()
        query = """
        SELECT DISTINCT ON (optic_name) *
        FROM risk_assessments 
        WHERE project_id = $1
        ORDER BY optic_name, rating_date DESC
        """
        rows = await connection.fetch(query, project_id)

        if not rows:
            raise HTTPException(status_code=404, detail="Project not found")

        return [
            RiskAssessment(
                id=row["id"],
                project_id=row["project_id"],
                project_name=row["project_name"],
                rating_date=row["rating_date"],
                optic_name=row["optic_name"],
                rating=row["rating"],
                justification=row["justification"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest assessments for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest assessments")
    finally:
        if connection:
            await connection.close()


@app.get("/api/projects/{project_id}/trends", response_model=List[ProjectTrend])
async def get_project_trends(project_id: str, optic_name: Optional[str] = None):
    """Get trend data for a project (all assessments over time)."""
    connection = None
    try:
        connection = await get_db_connection()
        
        if optic_name:
            query = """
            SELECT rating_date, optic_name, rating
            FROM risk_assessments 
            WHERE project_id = $1 AND optic_name = $2
            ORDER BY rating_date, optic_name
            """
            rows = await connection.fetch(query, project_id, optic_name)
        else:
            query = """
            SELECT rating_date, optic_name, rating
            FROM risk_assessments 
            WHERE project_id = $1
            ORDER BY rating_date, optic_name
            """
            rows = await connection.fetch(query, project_id)
        
        return [
            ProjectTrend(
                rating_date=row["rating_date"],
                optic_name=row["optic_name"],
                rating=row["rating"],
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching trends for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching trends")
    finally:
        if connection:
            await connection.close()


@app.get("/health")
async def health_check():
    """Health check endpoint (async)."""
    connection = None
    try:
        connection = await get_db_connection()
        result = await connection.fetchval("SELECT 1")  # returns the first column of the first row
        return {"status": "healthy", "database": "connected", "result": result}
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")
    finally:
        if connection:
            await connection.close()
            
@app.post("/api/process-rows")
async def process_rows(request: ProcessRequest):
    """Process rows and insert/update in database."""
    connection = None
    try:
        logger.info("Process rows and insert/update in database...")
        connection = await get_db_connection()
        processed_count = 0

        for row_data in request.rows:
            success = await process_single_row(connection, row_data)
            if success:
                processed_count += 1

        return {
            "success": True,
            "rows_processed": processed_count,
            "total_rows": request.total_rows,
            "file_path": request.file_path,
        }

    except Exception as e:
        logger.error(f"Error processing rows: {e}")
        return {
            "success": False,
            "error": str(e),
            "file_path": request.file_path,
        }
    finally:
        if connection:
            await connection.close()



def parse_datetime(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None  # or handle differently
    
async def process_single_row(connection, row_data: RowData) -> bool:
    """Process a single row and insert/update in database."""
    try:
        updated_value = parse_datetime(row_data.updated)
        query = """
        
        INSERT INTO project_data (
            project_id, project_name, updated, portfolio_manager,
            executive_summary, comments_on_schedule, comments_on_budget,
            comments_on_cost, comments_on_resources, comments_on_scope,
            comments, key_activities_planned, last_month_achievements,
            business_value_comment, combined_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (project_id, project_name, updated) 
        DO UPDATE SET
            portfolio_manager = EXCLUDED.portfolio_manager,
            executive_summary = EXCLUDED.executive_summary,
            comments_on_schedule = EXCLUDED.comments_on_schedule,
            comments_on_budget = EXCLUDED.comments_on_budget,
            comments_on_cost = EXCLUDED.comments_on_cost,
            comments_on_resources = EXCLUDED.comments_on_resources,
            comments_on_scope = EXCLUDED.comments_on_scope,
            comments = EXCLUDED.comments,
            key_activities_planned = EXCLUDED.key_activities_planned,
            last_month_achievements = EXCLUDED.last_month_achievements,
            business_value_comment = EXCLUDED.business_value_comment,
            combined_data = EXCLUDED.combined_data,
            processed_at = CURRENT_TIMESTAMP
        """
        
        await connection.execute(
            query,
            row_data.project_id, row_data.project_name, updated_value,
            row_data.portfolio_manager, row_data.executive_summary,
            row_data.comments_on_schedule, row_data.comments_on_budget,
            row_data.comments_on_cost, row_data.comments_on_resources,
            row_data.comments_on_scope, row_data.comments,
            row_data.key_activities_planned, row_data.last_month_achievements,
            row_data.business_value_comment, row_data.combined_data
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing row {row_data.project_id}: {e}")
        return False

if __name__ == "__main__":
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )