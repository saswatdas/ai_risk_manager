import json
import pandas as pd
import risk_summary_db_ingestion

def generateReport(jsonpath: str):
    
    jsonpath = jsonpath  #"risk_analysis_results.json"
    
    with open(jsonpath, "r") as f:
        data = json.load(f)   # data is a list of dicts

    results = []
    final_agent =''
    final_project_id =''
    final_project_name =''
    final_rating_date =''
    final_optic_ratings =[]



    for task_item in data:
        tasks_output=task_item.get("tasks_output", {})
        
        #agent = tasks_output"agent", "").strip().lower()
    
        #print("tasks_output::::\n", tasks_output)
        #agents = [item.get("agent", "").strip() for item in tasks_output if "agent" in item]
        #print("agent>>", agents)
        for item in tasks_output:
            agent = item.get("agent", "").strip()
            #print("agent>>", agent)
            if agent == "Chief Risk Assessment Officer":
            
                json_dict = item.get("json_dict", {})  # nested object
                    
                project_id = json_dict.get("project_id")
                project_name = json_dict.get("project_name")
                rating_date = json_dict.get("rating_date")
                
                optic_ratings = json_dict.get("optic_ratings", [])

                final_agent =agent
                final_project_id =project_id
                final_project_name =project_name
                final_rating_date =rating_date
                final_optic_ratings =optic_ratings
                
    results.append({
                    "agent": agent,
                    "project_id": project_id,
                    "project_name": project_name,
                    "rating_date": rating_date,
                    "optic_ratings": optic_ratings
                })    
    # Print sample
    for res in results[:2]:
        #print("SUMMARY:")
        #print(json.dumps(res, indent=2))

        #saving the data to a spreadsheet
        rows = []
        for optic in res["optic_ratings"]:
            rows.append({
                "project_id": res["project_id"],
                "project_name": res["project_name"],
                "rating_date": res["rating_date"],
                "optic_name": optic["optic_name"],
                "rating": optic["rating"],
                "justification": optic["justification"]
            })

        # Create DataFrame
        df = pd.DataFrame(rows)

        # Save to Excel
        output_file = "risk_analysis_report.xlsx"
        df.to_excel(output_file, index=False)

        print(f"Excel file saved as {output_file}")
        
        #Save to Postgresql DB
        risk_summary_db_ingestion.process_data_to_database(final_project_id,rows)