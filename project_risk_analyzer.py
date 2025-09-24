# db_parser.py
import asyncpg
import asyncio
from typing import List, Dict, Any
import logging
import risk_assesment_output  # Your existing output module
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

class ProjectStatusParserDB:
    def __init__(self):
        self.projects = []

    async def get_projects_from_db(self) -> List[Dict[str, Any]]:
        """
        Query the PostgreSQL database and return projects for analysis.
        """
        conn = None
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            query = """
                SELECT 
                    project_id,
                    project_name,
                    updated,
                    combined_data,
                    portfolio_manager,
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
                FROM project_data
                ORDER BY updated DESC
            """
            rows = await conn.fetch(query)

            for row in rows:
                # Convert asyncpg Record to dict
                row_dict = dict(row)
                project_text = self.create_project_text(row_dict)

                # Skip rows without meaningful content
                if not project_text.strip():
                    continue

                self.projects.append({
                    'project_id': row_dict['project_id'],
                    'project_name': row_dict['project_name'],
                    'project_text': project_text,
                    'row_data': row_dict
                })

            logger.info(f"Fetched {len(self.projects)} projects from database")
            return self.projects

        except Exception as e:
            logger.error(f"Error fetching projects from DB: {e}")
            return []
        finally:
            if conn:
                await conn.close()

    def _extract_text_content(self, text) -> str:
        """Clean HTML/text content."""
        if not text:
            return ''
        import re
        text_str = str(text)
        clean_text = re.sub(r'<[^>]*>', '', text_str)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text

    def create_project_text(self, row: Dict[str, Any]) -> str:
        """Concatenate project text for AI analysis."""
        text_parts = []

        sections = [
            ('EXECUTIVE SUMMARY', 'executive_summary'),
            ('COMMENTS ON SCHEDULE', 'comments_on_schedule'),
            ('COMMENTS ON BUDGET', 'comments_on_budget'),
            ('COMMENTS ON COST', 'comments_on_cost'),
            ('COMMENTS ON RESOURCES', 'comments_on_resources'),
            ('COMMENTS ON SCOPE', 'comments_on_scope'),
            ('GENERAL COMMENTS', 'comments'),
            ('KEY ACTIVITIES PLANNED', 'key_activities_planned'),
            ('LAST MONTH ACHIEVEMENTS', 'last_month_achievements'),
            ('BUSINESS VALUE', 'business_value_comment'),
            ('LAST UPDATED', 'updated')
        ]

        for section_name, col in sections:
            content = self._extract_text_content(row.get(col, ''))
            if content.lower() not in ['n/a', 'na', 'none', '']:
                text_parts.append(f"--- {section_name} ---\n{content}\n")

        # Phase info
        phase = row.get('phase', '')
        if phase:
            text_parts.append(f"PROJECT PHASE: {phase}")

        # Date info
        start_date = row.get('planned_start_date', None)
        end_date = row.get('planned_end_date', None)
        if start_date:
            text_parts.append(f"PLANNED START DATE: {start_date}")
        if end_date:
            text_parts.append(f"PLANNED END DATE: {end_date}")

        return "\n".join(text_parts)

    async def process_projects_for_crew(self) -> List[Dict[str, Any]]:
        """Main entry: fetch projects and prepare for CrewAI."""
        return await self.get_projects_from_db()


# Example usage
async def main():
    parser = ProjectStatusParserDB()
    projects = await parser.process_projects_for_crew()

    if not projects:
        print("No projects to analyze")
        return

    from projectmanager_assistant import ProjectRiskCrew

    all_results = []
    project_text_accum = ''
    project_ids = set()

    for project_data in projects:
        pid = project_data['project_id']
        if pid not in project_ids:
            project_text_accum += project_data['project_text'] + "\n"
            project_ids.add(pid)

        crew = ProjectRiskCrew(
            project_id=pid,
            project_name=project_data['project_name'],
            project_text=project_text_accum
        )
        result = crew.run()
        all_results.append(result)

    # Save results
    import json
    with open('risk_analysis_results_new.json', 'w') as f:
        json.dump([res.dict() if hasattr(res, 'dict') else res for res in all_results], f, indent=2)

    print("Results saved to risk_analysis_results_new.json")
    print("Generating final output...")
    #risk_assesment_output.generateReport('risk_analysis_results_new.json')
    generate_final_output()

def generate_final_output():
        
        results = risk_assesment_output.generateReport('risk_analysis_results_new.json')
        
        if(results):
            print("New Output generated successfully...")

if __name__ == "__main__":
    asyncio.run(main())
