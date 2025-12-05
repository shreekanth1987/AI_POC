import os
from datetime import datetime
from uuid import uuid4
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
    InputDataset,
    OutputDataset,
)
from openlineage.client.facet import SchemaDatasetFacet, SchemaField, SqlJobFacet

# Configure to write to file (JSON format)
os.environ["OPENLINEAGE_CONFIG"] = "openlineage.yml"

def create_lineage_events():
    """Generate sample OpenLineage events with column-level lineage"""
    
    # Initialize client to write to file
    client = OpenLineageClient.from_environment()
    
    # Job and run identifiers
    namespace = "my_data_pipeline"
    job_name = "customer_analytics_etl"
    run_id = str(uuid4())
    event_time = datetime.now().isoformat()
    
    # Define input dataset with schema
    input_dataset = InputDataset(
        namespace=namespace,
        name="raw_customers",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="customer_id", type="INTEGER"),
                    SchemaField(name="first_name", type="VARCHAR"),
                    SchemaField(name="last_name", type="VARCHAR"),
                    SchemaField(name="email", type="VARCHAR"),
                    SchemaField(name="purchase_amount", type="DECIMAL"),
                ]
            )
        }
    )
    
    # Define output dataset with schema
    output_dataset = OutputDataset(
        namespace=namespace,
        name="analytics_customer_summary",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="customer_id", type="INTEGER"),
                    SchemaField(name="full_name", type="VARCHAR"),
                    SchemaField(name="email_normalized", type="VARCHAR"),
                    SchemaField(name="total_spent", type="DECIMAL"),
                ]
            )
        }
    )
    
    # Create SQL transformation facet
    sql_facet = SqlJobFacet(
        query="""
        INSERT INTO analytics_customer_summary
        SELECT 
            customer_id,
            CONCAT(first_name, ' ', last_name) AS full_name,
            UPPER(email) AS email_normalized,
            SUM(purchase_amount) AS total_spent
        FROM raw_customers
        GROUP BY customer_id, first_name, last_name, email
        """
    )
    
    # Create job with facets
    job = Job(
        namespace=namespace,
        name=job_name,
        facets={"sql": sql_facet}
    )
    
    # Create run
    run = Run(runId=run_id, facets={})
    
    # Emit START event
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=event_time,
        run=run,
        job=job,
        producer="https://github.com/my-org/my-pipeline",
        inputs=[input_dataset],
        outputs=[output_dataset],
    )
    
    print(f"Emitting START event for run: {run_id}")
    client.emit(start_event)
    
    # Simulate job processing
    import time
    time.sleep(1)
    
    # Emit COMPLETE event
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now().isoformat(),
        run=run,
        job=job,
        producer="https://github.com/my-org/my-pipeline",
        inputs=[input_dataset],
        outputs=[output_dataset],
    )
    
    print(f"Emitting COMPLETE event for run: {run_id}")
    client.emit(complete_event)
    
    print(f"\nLineage events successfully written to file!")
    return run_id

if __name__ == "__main__":
    # Run the example
    run_id = create_lineage_events()
    print(f"\nRun ID: {run_id}")
    print("Check the output file for OpenLineage JSON events")
