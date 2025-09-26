import pandas as pd

def simple_process_status_report(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name='Sheet1')
    
    print("Original data sample:")
    print(df[['Project Name', 'Number', 'Updated']].head())
    
    # Convert Updated column to datetime
    df['Updated'] = pd.to_datetime(df['Updated'])
    
    # Define text columns
    text_columns = [
        'Executive Summary', 'Business Value Comment', 'Comments', 
        'Comments on Budget', 'Comments on Cost', 'Comments on Resources', 
        'Comments on Schedule', 'Comments on Scope', 'Key Activities planned', 
        "Last Month's Achievements"
    ]
    
    # Simple text extraction - just convert to string
    def simple_extract_text(content):
        if pd.isna(content):
            return ""
        return str(content).strip()
    
    # Process each project group
    result_rows = []
    
    for (project_name, project_number), group in df.groupby(['Project Name', 'Number']):
        print(f"Processing {project_name} - {project_number}, records: {len(group)}")
        
        # Get latest record
        latest_record = group.loc[group['Updated'].idxmax()]
        
        # Simply concatenate all text fields from all records
        all_texts = []
        for _, record in group.iterrows():
            for col in text_columns:
                if col in record:
                    text = simple_extract_text(record[col])
                    if text and text not in ['-', 'n/a', 'nan', 'None', '']:
                        all_texts.append(f"{col}: {text}")
        
        project_text = "\n".join(all_texts) if all_texts else "No content"
        
        result_rows.append({
            'Project Name': project_name,
            'Number': project_number,
            'Portfolio manager': latest_record.get('Portfolio manager', ''),
            'Latest Update': latest_record['Updated'],
            'Total Records': len(group),
            'project_text': project_text
        })
    
    result_df = pd.DataFrame(result_rows)
    return result_df

# Test with simple version
if __name__ == "__main__":
    simple_result = simple_process_status_report('status_report.xlsx')
    print("\nSimple processing results:")
    print(simple_result[['Project Name', 'Number', 'Latest Update', 'Total Records', 'project_text']])
    
    # Check if any project_text was extracted
    """
    for idx, row in simple_result.iterrows():
        print(f"\n{row['Project Name']} - Text length: {len(row['project_text'])}")
        if len(row['project_text']) > 50:
            print(f"Sample: {row['project_text'][:100]}...")
    """