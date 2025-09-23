# excel_parser.py
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
import risk_assesment_output

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProjectStatusParser:
    def __init__(self, excel_file_path: str, sheet_name: Optional[str] = None):
        """
        Initialize the parser with the Excel file path.
        
        Args:
            excel_file_path (str): Path to the Excel file
            sheet_name (str, optional): Specific sheet name to read. If None, reads first sheet.
        """
        self.excel_file_path = excel_file_path
        self.sheet_name = sheet_name
        self.df = None
        self.required_columns = [
            'Project Name', 'Number', 'Executive Summary', 'Planned end date',
            'Planned start date', 'Updated', 'Phase', 'Business Value Comment',
            'Comments', 'Comments on Budget', 'Comments on Cost', 
            'Comments on Resources', 'Comments on Schedule', 'Comments on Scope',
            'Key Activities planned', 'Last Month\'s Achievements'
        ]
    
    def load_and_validate_data(self) -> bool:
        """
        Load the Excel file and validate that required columns exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read the Excel file
            if self.sheet_name:
                self.df = pd.read_excel(self.excel_file_path, sheet_name=self.sheet_name)
            else:
                self.df = pd.read_excel(self.excel_file_path)
            
            logger.info(f"Successfully loaded Excel file. Shape: {self.df.shape}")
            
            # Check for required columns
            missing_columns = [col for col in self.required_columns if col not in self.df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
                
            # Basic data cleaning
            self._clean_data()
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return False
    
    def _clean_data(self):
        """Perform basic data cleaning operations."""
        # Replace NaN values with empty strings for text columns
        text_columns = [col for col in self.required_columns if col not in ['Planned end date', 'Planned start date', 'Updated']]
        for col in text_columns:
            self.df[col] = self.df[col].fillna('')
        
        # Convert date columns to proper format
        date_columns = ['Planned end date', 'Planned start date', 'Updated']
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
    
    def _extract_text_content(self, text) -> str:
        """
        Extract text content from HTML-like formatted cells.
        
        Args:
            text: Cell content that may contain HTML tags
            
        Returns:
            str: Clean text content
        """
        if pd.isna(text) or text == '':
            return ''
        
        text_str = str(text)
        # Simple HTML tag removal (for cases like <p>, <li>, etc.)
        import re
        clean_text = re.sub(r'<[^>]*>', '', text_str)
        # Replace multiple spaces/newlines with single space
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text
    
    def create_project_text(self, row: pd.Series) -> str:
        """
        Create concatenated project text from relevant columns for AI analysis.
        
        Args:
            row (pd.Series): A row from the DataFrame
            
        Returns:
            str: Concatenated text for AI analysis
        """
        text_parts = []
        
        # Add key sections with headers for context
        sections = [
            ('EXECUTIVE SUMMARY', 'Executive Summary'),
            ('COMMENTS ON SCHEDULE', 'Comments on Schedule'),
            ('COMMENTS ON BUDGET', 'Comments on Budget'),
            ('COMMENTS ON COST', 'Comments on Cost'),
            ('COMMENTS ON RESOURCES', 'Comments on Resources'),
            ('COMMENTS ON SCOPE', 'Comments on Scope'),
            ('GENERAL COMMENTS', 'Comments'),
            ('KEY ACTIVITIES PLANNED', 'Key Activities planned'),
            ('LAST MONTH ACHIEVEMENTS', 'Last Month\'s Achievements'),
            ('BUSINESS VALUE', 'Business Value Comment'),
            ('LAST UPDATED', 'Updated')
        ]
        
        for section_name, column_name in sections:
            content = self._extract_text_content(row[column_name])
            if content and content.lower() not in ['n/a', 'na', 'none', '']:
                text_parts.append(f"--- {section_name} ---\n{content}\n")
        
        # Add phase information
        phase = row.get('Phase', '')
        if phase:
            text_parts.append(f"PROJECT PHASE: {phase}")
            
        #Add last udpated date
        last_updated = row.get('Updated', '')
        if last_updated:
            text_parts.append(f"LAST UPDATED: {last_updated}")
        # Add date context
        start_date = row.get('Planned start date', '')
        end_date = row.get('Planned end date', '')
        if not pd.isna(start_date):
            text_parts.append(f"PLANNED START DATE: {start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date}")
        if not pd.isna(end_date):
            text_parts.append(f"PLANNED END DATE: {end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date}")
        
        return "\n".join(text_parts)
    
    def get_projects_for_analysis(self) -> List[Dict[str, Any]]:
        """
        Extract all projects from the Excel file formatted for AI analysis.
        
        Returns:
            List[Dict]: List of projects with id, name, and formatted text
        """
        if self.df is None:
            if not self.load_and_validate_data():
                return []
        
        projects = []
        
        for index, row in self.df.iterrows():
            try:
                project_id = str(row['Number']).strip()
                project_name = str(row['Project Name']).strip()
                
                # Skip rows with missing essential data
                if not project_id or project_id == 'nan' or not project_name or project_name == 'nan':
                    logger.warning(f"Skipping row {index + 2} due to missing project ID or name")
                    continue
                
                project_text = self.create_project_text(row)
                
                # Skip if no meaningful text content
                if not project_text.strip():
                    logger.warning(f"Skipping project {project_id} - no analyzable content")
                    continue
                
                projects.append({
                    'project_id': project_id,
                    'project_name': project_name,
                    'project_text': project_text,
                    'row_data': row.to_dict()  # Keep original data for reference
                })
                
                logger.info(f"Prepared project {project_id} for analysis")
                
            except Exception as e:
                logger.error(f"Error processing row {index + 2}: {e}")
                #continue
        
        return projects
    
    def process_excel_to_crew_input(self) -> List[Dict[str, str]]:
        """
        Main method to process Excel file and return data ready for ProjectRiskCrew.
        
        Returns:
            List[Dict]: List of projects in format for ProjectRiskCrew
        """
        projects = self.get_projects_for_analysis()
        
        if not projects:
            logger.warning("No valid projects found for analysis")
            return []
        
        logger.info(f"Successfully prepared {len(projects)} projects for risk analysis")
        return projects
def extract_risk_officer_summary(final_result):
    """
    Extract the Chief Risk Assessment Officer's summary from the final result.
    
    Args:
        final_result: The ProjectRating object returned by crew.kickoff()
    
    Returns:
        str: The comprehensive risk assessment summary
    """
    if hasattr(final_result, 'raw') and final_result.raw:
        # If the result has a raw attribute (typical CrewAI output)
        return final_result.raw
    elif hasattr(final_result, 'dict'):
        # Convert to dict and extract information
        result_dict = final_result.dict()
        summary = f"Risk Assessment Summary for {result_dict['project_name']} ({result_dict['project_id']})\n"
        summary += f"Assessment Date: {result_dict['rating_date']}\n\n"
        
        for rating in result_dict['optic_ratings']:
            summary += f"{rating['optic_name']}: {rating['rating']}\n"
            summary += f"Justification: {rating['justification']}\n"
            summary += "-" * 50 + "\n"
        
        return summary
    else:
        # Handle string or other formats
        return str(final_result)
# Example usage and integration with ProjectRiskCrew
def main():
    # Initialize the parser
    excel_file_path = "status_report.xlsx"  # Update this path
    parser = ProjectStatusParser(excel_file_path)
    
    # Process the Excel file
    projects_for_analysis = parser.process_excel_to_crew_input()
    
    if not projects_for_analysis:
        print("No projects to analyze. Exiting.")
        return
    
    # Now process each project through the CrewAI system
    from projectmanager_assistant import ProjectRiskCrew  # Import your CrewAI class
    
    all_results = []
    project_ids=[]
    project_text=''
    for project_data in projects_for_analysis:
        print(f"\n{'='*50}")
        print(f"Analyzing Project: {project_data['project_name']} ({project_data['project_id']})")
        print(f"{'='*50}")
        project_ids.append(project_data['project_id'])
        if(project_data['project_id'] in project_ids):
            project_text=project_text+project_data['project_text']
       
            
            #print("project_text :::::: ",project_text)
            
        # Initialize and run the crew for this project     
        crew = ProjectRiskCrew(
                project_id=project_data['project_id'],
                project_name=project_data['project_name'],
                project_text=project_text
            )
            
        result = crew.run()
        all_results.append(result)
        """
        project_summary =extract_risk_officer_summary (all_results)  
        
        print("CHIEF RISK ASSESSMENT OFFICER SUMMARY:")
        print("=" * 60)
        print("PROJECT SUMMARY: \n", project_summary)
        print("=" * 60)
        """
        #print(f"Analysis completed for {project_data['project_id']}")
        #Overall health	Resources	Schedule	Scope	Cost	Value
            
        
    
    # Save or process the results
    #print(f"\nCompleted analysis of {len(all_results)} projects")
     
    # You could save results to JSON, database, etc.
    import json
    with open('risk_analysis_results.json', 'w') as f:
        json.dump([result.dict() if hasattr(result, 'dict') else result for result in all_results], f, indent=2)
    
    print("Results saved to risk_analysis_results.json")
    
    print("Calling generate_final_output to generate output excel")
    generate_final_output()
    
    
def generate_final_output():
        
        results = risk_assesment_output.generateReport('risk_analysis_results.json')
        
        if(results):
            print("Output generated successfully...")
        
        """
        # Print or save the results
        for res in results:
            print(json.dumps(res, indent=2))
        """
    
if __name__ == "__main__":
    main()
    #generate_final_output()