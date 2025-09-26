import os,yaml
from crewai import Agent, Task, Crew, Process
from textwrap import dedent
from pydantic import BaseModel, Field
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
os.environ['OPENAI_MODEL_NAME'] = 'gpt-4o'

# Define file paths for YAML configurations
files = {
    'agents': 'config/agents.yaml',
    'tasks': 'config/tasks.yaml',
    'optics_knowledge_base': 'config/optics_knowledge_base.yaml'
}


# Load configurations from YAML files
configs = {}
for config_type, file_path in files.items():
    with open(file_path, 'r') as file:
        configs[config_type] = yaml.safe_load(file)

# Assign loaded configurations to specific variables
agents_config = configs['agents']
tasks_config = configs['tasks']
optics_knowledgebase_config =configs['optics_knowledge_base']

# Define your Pydantic model for the structured output
class OpticRating(BaseModel):
    optic_name: str = Field(..., description="The name of the optic being rated.")
    rating: str = Field(..., description="The rating for the optic: Red, Amber, or Green.")
    justification: str = Field(..., description="The direct/inferred quote from the text that supports the rating.")
    recommendation: str = Field(..., description="The direct/inferred quote from the text that supports the rating.")

class ProjectRating(BaseModel):
    project_id: str = Field(..., description="The unique ID of the project.")
    project_name: str = Field(..., description="The name of the project.")
    rating_date: str = Field(..., description="The date of the rating analysis.")
    optic_ratings: List[OpticRating] = Field(..., description="List of ratings for all 12 optics.")
    

class ProjectRiskCrew:
    def __init__(self, project_id, project_name, project_text):
        self.project_id = project_id
        self.project_name = project_name
        self.project_text = project_text
        
    def run(self):
        # 1. Create Specialist Agents and Tasks for each optic
        agents = []
        tasks = []
        #print("optics_knowledgebase_config.items()>>", optics_knowledgebase_config.items())
        for optic_name, criteria in optics_knowledgebase_config.items():
            #agent = self.create_specialist_agent(optic_name, criteria)
            #task = self.create_specialist_task(agent, optic_name, criteria)
            #
            # print("criteria???", criteria)
            context ={
                "project_id" : self.project_id,
                "project_name" : self.project_name,
                "project_text" : self.project_text,
                "optic_name" : optic_name,
                "criteria_green": criteria["Green"],
                "criteria_amber": criteria["Amber"],
                "criteria_red": criteria["Red"],
                "today" : datetime.now().strftime("%Y-%m-%d")
            }
            print('optic_name>>>>>>>', optic_name)
            # Inject variables into description & expected_output of Task
            task_template = tasks_config[optic_name]
            print('task_template>>>>>>>', task_template)
            
            description = task_template["description"].format(**context)
            expected_output = task_template["expected_output"].format(**context) 
            
            #Inject variables into Agent fields
            agent_template= agents_config[optic_name]
            
            role = agent_template["role"].format(**context)
            goal = agent_template["goal"].format(**context)
            backstory = agent_template["backstory"].format(**context)

                
            specialized_agent=Agent(
                role=role,
                goal=goal,
                backstory=backstory
            )
            
            task = Task(
                #config=tasks_config[optic_name],
                description= description,
                expected_output = expected_output,
                agent= specialized_agent,
                output_json= OpticRating 
            )
            print('specialized_agent.....',specialized_agent)
            print('task.....',task)
            agents.append(specialized_agent)
            tasks.append(task)
            
            
        #creating agents        
        """
        firmness_of_opportunity_agent = Agent(
            config=agents_config['firmness_of_opportunity_agent']
        )
        """
        risk_manager_agent= Agent(
            config=agents_config['risk_manager_agent']
        )
        
        #creating tasks
        """
        firmness_of_opportunity_task = Task(
            config=tasks_config['firmness_of_opportunity'],
            agent=firmness_of_opportunity_agent
        )
        """
        final_task_config=tasks_config['risk_consolidation']
        
        final_task_description = final_task_config["description"].format(**context)
        final_task_expected_output = final_task_config["expected_output"].format(**context) 
        
        final_task = Task(
            description= final_task_description,
            expected_output = final_task_expected_output,
            agent=risk_manager_agent,
            output_json=ProjectRating, # Validates the entire final output
            context=tasks
        )
        
        project_crew = Crew(
            agents=agents + [risk_manager_agent], # All agents
            tasks=tasks + [final_task], # All tasks
            process=Process.sequential, # Tasks are executed in order. Specialists first, then manager.
            verbose=True,
        )
        
        final_result = project_crew.kickoff()
        return final_result
    
    """
    if __name__ == "__main__":
    # This data would come from your n8n workflow or Excel parser
    sample_project_id = "PRJ0016435
    """
    