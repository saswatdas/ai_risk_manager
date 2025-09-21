# main_integration.py
from database_handler import RiskAssessmentDB
import json

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'mydb',
    'user': 'myuser',
    'password': 'password@123',
    'port': 5432
}

def process_data_to_database(project_id,data):
    """Process a project and store results in database."""
    
    # Initialize database handler
    db_handler = RiskAssessmentDB(DB_CONFIG)
    print("db_handler>", db_handler)
    assessments_data =[]
    try:
        
        assessments_data=data
        # Insert into database
        if assessments_data:
            success = db_handler.bulk_insert_risk_assessments(assessments_data)
            if success:
                print(f"Successfully stored {len(assessments_data)} risk assessments for {project_id}")
            else:
                print(f"Failed to store risk assessments for {project_id}")
        
        return assessments_data
        
    except Exception as e:
        print(f"Error processing project {project_id}: {e}")
        return []
    finally:
        db_handler.close()


def insert_to_db(project_id,results):
    
     # Process each project and store in database
    process_data_to_database(project_id,results)
    """
    for data in results:
        process_data_to_database(
            data['project_id'],
            data['project_name'],
            data['rating_date'],
            data['optic_name'],
            data['rating'],
            data['justification']
        )
    """
    
if __name__ == "__main__":
    results=[]
    project_id=''
    insert_to_db(project_id,results)