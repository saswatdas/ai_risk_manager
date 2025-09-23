# backend/file_watcher_simple.py
import os
import time
import hashlib
import requests
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
import logging
import threading

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
        self.file_hashes = {}
        self.processing_lock = threading.Lock()
    
    def on_created(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            threading.Thread(target=self.process_file_sync, args=(event.src_path, "created"), daemon=True).start()
    
    def on_modified(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            threading.Thread(target=self.process_file_sync, args=(event.src_path, "modified"), daemon=True).start()
    
    def is_excel_file(self, file_path):
        return file_path.lower().endswith(('.xlsx', '.xls'))
    
    def process_file_sync(self, file_path, event_type):
        """Process file synchronously in a separate thread."""
        with self.processing_lock:
            try:
                file_hash = self.calculate_file_hash(file_path)
                
                # Skip if file hasn't changed
                if file_path in self.file_hashes and self.file_hashes[file_path] == file_hash:
                    return
                
                self.file_hashes[file_path] = file_hash
                
                logger.info(f"Processing {event_type} file: {file_path}")
                
                # Extract rows from Excel
                rows_data = self.extract_rows_from_excel(file_path)
                
                if rows_data:
                    # Send to API service
                    self.send_to_api_service(rows_data, file_path)
                else:
                    logger.warning(f"No valid rows found in {file_path}")
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
    
    def calculate_file_hash(self, file_path):
        """Calculate MD5 hash of file content."""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def extract_rows_from_excel(self, file_path):
        """Extract rows from Excel file and return as list of dictionaries."""
        try:
            df = pd.read_excel(file_path)
            required_columns = ['Number', 'Project Name', 'Updated']
            
            # Validate required columns
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in {file_path}")
                return []
            
            rows_data = []
            for _, row in df.iterrows():
                row_data = self.extract_row_data(row)
                if row_data:  # Only add valid rows
                    rows_data.append(row_data)
            
            return rows_data
            
        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {e}")
            return []
    
    def extract_row_data(self, row):
        """Extract and format data from a single row."""
        try:
            # Primary key columns
            project_id = self.safe_get(row, 'Number')
            project_name = self.safe_get(row, 'Project Name')
            updated = self.safe_get(row, 'Updated')
            
            # Skip if primary key data is missing
            if not all([project_id, project_name, updated]):
                return None
            
            # Format all required columns
            formatted_data = {
                'project_id': project_id.strip(),
                'project_name': project_name.strip(),
                'updated': updated,
                'portfolio_manager': self.safe_get(row, 'Portfolio Manager'),
                'executive_summary': self.format_section('Executive Summary', row.get('Executive Summary')),
                'comments_on_schedule': self.format_section('Comments on Schedule', row.get('Comments on Schedule')),
                'comments_on_budget': self.format_section('Comments on Budget', row.get('Comments on Budget')),
                'comments_on_cost': self.format_section('Comments on Cost', row.get('Comments on Cost')),
                'comments_on_resources': self.format_section('Comments on Resources', row.get('Comments on Resources')),
                'comments_on_scope': self.format_section('Comments on Scope', row.get('Comments on Scope')),
                'comments': self.format_section('Comments', row.get('Comments')),
                'key_activities_planned': self.format_section('Key Activities Planned', row.get('Key Activities planned')),
                'last_month_achievements': self.format_section('Last Month Achievements', row.get('Last Month\'s Achievements')),
                'business_value_comment': self.format_section('Business Value Comment', row.get('Business Value Comment'))
            }
            
            # Generate combined_data field
            sections = [
                formatted_data['executive_summary'],
                formatted_data['comments_on_schedule'],
                formatted_data['comments_on_budget'],
                formatted_data['comments_on_cost'],
                formatted_data['comments_on_resources'],
                formatted_data['comments_on_scope'],
                formatted_data['comments'],
                formatted_data['key_activities_planned'],
                formatted_data['last_month_achievements'],
                formatted_data['business_value_comment']
            ]
            
            formatted_data['combined_data'] = "\n".join([s for s in sections if s.strip()])
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error extracting row data: {e}")
            return None
    
    def safe_get(self, row, column_name, default=''):
        """Safely get value from DataFrame row."""
        try:
            value = row[column_name]
            return '' if pd.isna(value) else str(value).strip()
        except (KeyError, AttributeError):
            return default
    
    def format_section(self, section_name, content):
        """Format a section with header and content."""
        content_str = self.safe_get({'content': content}, 'content')
        return f"{section_name}: {content_str}" if content_str else ""
    
    def send_to_api_service(self, rows_data, file_path):
        """Send extracted rows to API service synchronously."""
        try:
            payload = {
                'file_path': file_path,
                'rows': rows_data,
                'total_rows': len(rows_data)
            }
            
            response = requests.post(
                f"{self.api_base_url}/api/process-rows",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                self.handle_api_response(result, file_path)
            else:
                logger.error(f"API error for {file_path}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending data to API for {file_path}: {e}")
    
    def handle_api_response(self, response, file_path):
        """Handle API response callback."""
        if response.get('success'):
            logger.info(f"✅ Successfully processed {response.get('rows_processed', 0)} rows from {file_path}")
        else:
            logger.error(f"❌ Failed to process {file_path}: {response.get('error')}")

class FileWatcher:
    def __init__(self):
        self.watch_directories = os.getenv('WATCH_DIRECTORIES', '').split(',')
        self.recursive = os.getenv('RECURSIVE_WATCH', 'true').lower() == 'true'
        self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000')
        self.observer = Observer()
        self.event_handler = ExcelFileHandler(self.api_base_url)
    
    def start_watching(self):
        """Start watching directories for file changes."""
        if not self.watch_directories or not self.watch_directories[0]:
            logger.error("No watch directories configured")
            return
        
        for directory in self.watch_directories:
            directory = directory.strip()
            if directory and os.path.exists(directory):
                self.observer.schedule(
                    self.event_handler, 
                    directory, 
                    recursive=self.recursive
                )
                logger.info(f"🔍 Watching directory: {directory} (recursive: {self.recursive})")
            else:
                logger.warning(f"Directory does not exist: {directory}")
        
        self.observer.start()
        logger.info("🚀 File watcher started successfully")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("🛑 File watcher stopped")
        
        self.observer.join()

def main():
    watcher = FileWatcher()
    watcher.start_watching()

if __name__ == "__main__":
    main()