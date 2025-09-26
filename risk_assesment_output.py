import json
import os
import pandas as pd
import risk_summary_db_ingestion

def generateReport(jsonpath: str):
    jsonfile = jsonpath
    print("Processing file:", jsonfile)
    
    with open(jsonfile, "r") as f:
        data = json.load(f)   # data is a list of dicts

    results = []

    for task_item in data:
        tasks_output = task_item.get("tasks_output", [])
        
        for item in tasks_output:
            agent = item.get("agent", "").strip()
            
            if agent == "Chief Risk Assessment Officer":
                json_dict = item.get("json_dict", {})
                
                project_id = json_dict.get("project_id")
                project_name = json_dict.get("project_name")
                rating_date = json_dict.get("rating_date")
                optic_ratings = json_dict.get("optic_ratings", [])
                
                # Append EACH project to results
                results.append({
                    "agent": agent,
                    "project_id": project_id,
                    "project_name": project_name,
                    "rating_date": rating_date,
                    "optic_ratings": optic_ratings
                })

    # Print summary
    print(f"Found {len(results)} projects")
    
    # Process all results
    all_rows = []
    for res in results:
        print(f"Processing project: {res['project_id']} - {res['project_name']}")
        
        for optic in res["optic_ratings"]:
            all_rows.append({
                "project_id": res["project_id"],
                "project_name": res["project_name"],
                "rating_date": res["rating_date"],
                "optic_name": optic["optic_name"],
                "rating": optic["rating"],
                "justification": optic["justification"],
                "recommendation": optic["recommendation"]
            })

    # Create DataFrame from ALL rows
    if all_rows:
        df = pd.DataFrame(all_rows)
        
        # MODIFICATION: Pick the last record for each combination of project_id, project_name, optic_name
        # Sort by rating_date to ensure we get the most recent record
        df['rating_date'] = pd.to_datetime(df['rating_date'])
        df = df.sort_values('rating_date')
        
        # Group by project_id, project_name, optic_name and take the last record
        df_last_records = df.groupby(['project_id', 'project_name', 'optic_name']).last().reset_index()
        
        print(f"Original records: {len(df)}")
        print(f"Unique records after picking last: {len(df_last_records)}")

        # Determine output filename
        output_file = jsonfile
        base, ext = os.path.splitext(output_file)
        if ext == ".json":
            output_file = base + ".xlsx"

        print(f"Output file: {output_file}")

        # Save to Excel (overwrite existing file) - using the filtered DataFrame
        df_last_records.to_excel(output_file, index=False)
        print(f"Excel file saved as {output_file} with {len(df_last_records)} records")
        
        # Save to PostgreSQL database - using the filtered data
        # Convert DataFrame back to list of dictionaries for database ingestion
        filtered_rows = df_last_records.to_dict('records')
        
        # Group by project_id for database ingestion
        projects_data = {}
        for row in filtered_rows:
            project_id = row['project_id']
            if project_id not in projects_data:
                projects_data[project_id] = []
            projects_data[project_id].append(row)
        
        for project_id, rows in projects_data.items():
            risk_summary_db_ingestion.process_data_to_database(project_id, rows)
            print(f"Saved project {project_id} to database (last records only)")
    else:
        print("No data found to process")

if __name__ == "__main__":
    generateReport("risk_analysis_report.json")