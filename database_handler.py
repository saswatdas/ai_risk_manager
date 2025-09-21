# database_handler.py
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict
import logging
from datetime import datetime

class RiskAssessmentDB:
    def __init__(self, db_config: Dict):
        
        #Initialize database connection.
        
        """
        db_config={
                    'host': 'localhost',
                    'database': 'mydb',
                    'user': 'myuser',
                    'password': 'password@123',
                    'port': 5432
                }
        """
        print("Initialize database connection...")
        self.db_config = db_config
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Successfully connected to PostgreSQL database...")
            logging.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")
    """
    def create_table(self):
        #Create the risk_assessments table if it doesn't exist.
        #create_table_sql = 
        CREATE TABLE IF NOT EXISTS risk_assessments (
            id SERIAL PRIMARY KEY,
            project_id VARCHAR(50) NOT NULL,
            project_name VARCHAR(255) NOT NULL,
            rating_date DATE NOT NULL,
            optic_name VARCHAR(100) NOT NULL,
            rating VARCHAR(10) NOT NULL CHECK (rating IN ('Red', 'Amber', 'Green')),
            justification TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (project_id, rating_date, optic_name)
        );
    
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.connection.commit()
                logging.info("Table created or already exists")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            self.connection.rollback()
            raise
    """
    def insert_risk_assessment(self, assessment_data: Dict):
        """
        Insert a single risk assessment record.
        
        Args:
            assessment_data: Dictionary with risk assessment data
        """
        insert_sql = """
        INSERT INTO risk_assessments 
        (project_id, project_name, rating_date, optic_name, rating, justification)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (project_id, rating_date, optic_name) 
        DO UPDATE SET 
            rating = EXCLUDED.rating,
            justification = EXCLUDED.justification,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    assessment_data['project_id'],
                    assessment_data['project_name'],
                    assessment_data['rating_date'],
                    assessment_data['optic_name'],
                    assessment_data['rating'],
                    assessment_data['justification']
                ))
                self.connection.commit()
                return True
        except Exception as e:
            logging.error(f"Error inserting risk assessment: {e}")
            self.connection.rollback()
            return False
    
    def bulk_insert_risk_assessments(self, assessments_data: List[Dict]):
        """
        Insert multiple risk assessment records efficiently.
        
        Args:
            assessments_data: List of dictionaries with risk assessment data
        """
        insert_sql = """
        INSERT INTO risk_assessments 
        (project_id, project_name, rating_date, optic_name, rating, justification)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (project_id, rating_date, optic_name) 
        DO UPDATE SET 
            rating = EXCLUDED.rating,
            justification = EXCLUDED.justification,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Prepare data for batch insertion
                data_tuples = [(
                    data['project_id'],
                    data['project_name'],
                    data['rating_date'],
                    data['optic_name'],
                    data['rating'],
                    data['justification']
                ) for data in assessments_data]
                
                execute_batch(cursor, insert_sql, data_tuples)
                self.connection.commit()
                print("Successfully inserted {len(assessments_data)} risk assessments")
                logging.info(f"Successfully inserted {len(assessments_data)} risk assessments")
                return True
        except Exception as e:
            print(f"Error bulk inserting risk assessments: {e}")
            logging.error(f"Error bulk inserting risk assessments: {e}")
            self.connection.rollback()
            return False
    
    def get_project_history(self, project_id: str) -> List[Dict]:
        """Get historical risk assessments for a specific project."""
        query_sql = """
        SELECT project_id, project_name, rating_date, optic_name, rating, justification
        FROM risk_assessments 
        WHERE project_id = %s 
        ORDER BY rating_date DESC, optic_name
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_sql, (project_id,))
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
        except Exception as e:
            logging.error(f"Error fetching project history: {e}")
            return []
    
    def get_latest_ratings(self, project_id: str, rating_date: str = None) -> List[Dict]:
        """Get the latest ratings for a project."""
        if rating_date:
            query_sql = """
            SELECT project_id, project_name, rating_date, optic_name, rating, justification
            FROM risk_assessments 
            WHERE project_id = %s AND rating_date = %s
            ORDER BY optic_name
            """
            params = (project_id, rating_date)
        else:
            query_sql = """
            SELECT DISTINCT ON (optic_name) 
                project_id, project_name, rating_date, optic_name, rating, justification
            FROM risk_assessments 
            WHERE project_id = %s 
            ORDER BY optic_name, rating_date DESC
            """
            params = (project_id,)
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_sql, params)
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
        except Exception as e:
            logging.error(f"Error fetching latest ratings: {e}")
            return []