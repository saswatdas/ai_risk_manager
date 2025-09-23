# backend/file_handler.py
import pandas as pd
import io
from datetime import datetime
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

def process_excel_file_complete(file_path: str) -> Dict[str, Any]:
    """
    Process complete Excel file and extract all records with all required columns.
    Returns summary of processing results.
    """
    connection = None
    try:
        connection = get_db_connection()
        
        # Read the Excel file
        df = pd.read_excel(file_path)
        logger.info(f"Read Excel file with {len(df)} rows")
        
        # Validate required columns
        required_columns = ['Number', 'Project Name', 'Updated']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg, "rows_processed": 0}
        
        processed_rows = 0
        skipped_rows = 0
        errors = []
        
        # Process each row in the Excel file
        for index, row in df.iterrows():
            try:
                result = process_single_row(connection, row)
                if result["success"]:
                    processed_rows += 1
                else:
                    skipped_rows += 1
                    errors.append(f"Row {index + 2}: {result['error']}")
                    
            except Exception as e:
                skipped_rows += 1
                error_msg = f"Row {index + 2}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        connection.commit()
        logger.info(f"Successfully processed {processed_rows} rows, skipped {skipped_rows} rows")
        
        return {
            "success": True,
            "rows_processed": processed_rows,
            "skipped_rows": skipped_rows,
            "total_rows": len(df),
            "errors": errors
        }
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Error processing Excel file {file_path}: {e}")
        return {"success": False, "error": str(e), "rows_processed": 0}
    finally:
        if connection:
            connection.close()

def process_single_row(connection, row) -> Dict[str, Any]:
    """Process a single row from Excel and insert into database."""
    # Extract primary key columns
    project_id = safe_get(row, 'Number', '').strip()
    project_name = safe_get(row, 'Project Name', '').strip()
    updated_value = safe_get(row, 'Updated')
    
    # Validate required fields
    if not project_id:
        return {"success": False, "error": "Missing project ID (Number)"}
    if not project_name:
        return {"success": False, "error": "Missing project name"}
    if not updated_value:
        return {"success": False, "error": "Missing updated date"}
    
    # Convert updated to datetime
    try:
        updated = pd.to_datetime(updated_value)
        if pd.isna(updated):
            return {"success": False, "error": "Invalid date format"}
    except Exception as e:
        return {"success": False, "error": f"Invalid date format: {str(e)}"}
    
    # Extract and format all required columns
    print("Portfolio manager",safe_get(row, 'Portfolio manager', '').strip())
    portfolio_manager = safe_get(row, 'Portfolio manager', '').strip()
    
    # Format each column with section headers
    executive_summary = format_section('Executive Summary', safe_get(row, 'Executive Summary'))
    comments_on_schedule = format_section('Comments on Schedule', safe_get(row, 'Comments on Schedule'))
    comments_on_budget = format_section('Comments on Budget', safe_get(row, 'Comments on Budget'))
    comments_on_cost = format_section('Comments on Cost', safe_get(row, 'Comments on Cost'))
    comments_on_resources = format_section('Comments on Resources', safe_get(row, 'Comments on Resources'))
    comments_on_scope = format_section('Comments on Scope', safe_get(row, 'Comments on Scope'))
    comments = format_section('Comments', safe_get(row, 'Comments'))
    key_activities_planned = format_section('Key Activities Planned', safe_get(row, 'Key Activities planned'))
    last_month_achievements = format_section('Last Month Achievements', safe_get(row, 'Last Month\'s Achievements'))
    business_value_comment = format_section('Business Value Comment', safe_get(row, 'Business Value Comment'))
    
    # Combine all sections into the data column
    combined_sections = [
        executive_summary,
        comments_on_schedule,
        comments_on_budget,
        comments_on_cost,
        comments_on_resources,
        comments_on_scope,
        comments,
        key_activities_planned,
        last_month_achievements,
        business_value_comment
    ]
    
    # Filter out empty sections and join with newlines
    combined_data = "\n".join([section for section in combined_sections if section.strip()])
    
    # Insert or update the data
    with connection.cursor() as cursor:
        query = """
        INSERT INTO project_data (
            project_id, project_name, updated, portfolio_manager,
            executive_summary, comments_on_schedule, comments_on_budget,
            comments_on_cost, comments_on_resources, comments_on_scope,
            comments, key_activities_planned, last_month_achievements,
            business_value_comment, combined_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
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
        
        cursor.execute(query, (
            project_id, project_name, updated, portfolio_manager,
            executive_summary, comments_on_schedule, comments_on_budget,
            comments_on_cost, comments_on_resources, comments_on_scope,
            comments, key_activities_planned, last_month_achievements,
            business_value_comment, combined_data
        ))
    
    return {"success": True}

def safe_get(row, column_name, default=''):
    """Safely get value from DataFrame row with proper null handling."""
    try:
        value = row[column_name]
        if pd.isna(value) or value is None:
            return default
        return str(value).strip()
    except (KeyError, AttributeError):
        return default

def format_section(section_name: str, content: str) -> str:
    """Format a section with header and content."""
    if not content or not content.strip():
        return ""
    return f"{section_name}: {content.strip()}"