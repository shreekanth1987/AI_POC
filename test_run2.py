import os
from datetime import datetime, timezone
from uuid import uuid4
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    RunEvent,
    RunState,
    Run,
    Job,
    InputDataset,
    OutputDataset,
)
from openlineage.client.facet import (
    SchemaDatasetFacet, 
    SchemaField, 
    SqlJobFacet,
    SourceCodeLocationJobFacet,
    NominalTimeRunFacet,
    ParentRunFacet,
    DataSourceDatasetFacet,
    LifecycleStateChangeDatasetFacet,
    DatasetVersionDatasetFacet,
    OwnershipDatasetFacet,
    OwnershipDatasetFacetOwners,
    ColumnLineageDatasetFacet,
    DataQualityMetricsInputDatasetFacet,
    DataQualityAssertionsDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    ColumnMetric,
    Assertion
)

# Configure OpenLineage
os.environ["OPENLINEAGE_CONFIG"] = "openlineage2.yml"


def create_comprehensive_lineage_event():
    """
    Create a complete OpenLineage RunEvent with multiple dataset facets
    demonstrating all major facet types
    """
    
    client = OpenLineageClient.from_environment()
    
    namespace = "production_warehouse"
    job_name = "customer_360_aggregation"
    run_id = str(uuid4())
    parent_run_id = str(uuid4())
    event_time = datetime.now(timezone.utc).isoformat()
    
    # ============================================
    # INPUT DATASET WITH COMPREHENSIVE FACETS
    # ============================================
    
    input_dataset = InputDataset(
        namespace="postgresql://prod-db:5432",
        name="ecommerce.public.raw_transactions",
        
        # Common dataset facets (in 'facets')
        facets={
            # Schema facet - defines structure
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(
                        name="transaction_id",
                        type="BIGINT",
                        description="Unique transaction identifier"
                    ),
                    SchemaField(
                        name="customer_id",
                        type="BIGINT",
                        description="Customer identifier"
                    ),
                    SchemaField(
                        name="product_name",
                        type="VARCHAR(255)",
                        description="Product name"
                    ),
                    SchemaField(
                        name="amount",
                        type="DECIMAL(10,2)",
                        description="Transaction amount"
                    ),
                    SchemaField(
                        name="transaction_date",
                        type="TIMESTAMP",
                        description="Transaction timestamp"
                    ),
                    SchemaField(
                        name="email",
                        type="VARCHAR(255)",
                        description="Customer email"
                    ),
                ]
            ),
            
            # Datasource facet - database connection info
            "dataSource": DataSourceDatasetFacet(
                name="prod-postgresql-01",
                uri="postgresql://prod-db.company.com:5432/ecommerce"
            ),
            
            # Version facet - dataset version tracking
            "version": DatasetVersionDatasetFacet(
                datasetVersion="2025-12-03T22:00:00Z"
            ),
            
            # Ownership facet - who owns this dataset
            "ownership": OwnershipDatasetFacet(
                owners=[
                    OwnershipDatasetFacetOwners(
                        name="data-engineering-team",
                        type="team"
                    ),
                    OwnershipDatasetFacetOwners(
                        name="john.doe@company.com",
                        type="person"
                    )
                ]
            ),
        },
        
        # Input-specific facets (in 'inputFacets')
        inputFacets={
            # Data quality metrics - profiling statistics
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                rowCount=150000,
                bytes=52428800,  # 50 MB
                fileCount=1,
                columnMetrics={
                    "transaction_id": ColumnMetric(
                        nullCount=0,
                        distinctCount=150000,
                        min=1.0,
                        max=150000.0,
                        count=150000
                    ),
                    "customer_id": ColumnMetric(
                        nullCount=0,
                        distinctCount=45000,
                        min=1.0,
                        max=100000.0,
                        count=150000
                    ),
                    "amount": ColumnMetric(
                        nullCount=150,
                        distinctCount=12500,
                        sum=7500000.0,
                        count=149850,
                        min=5.99,
                        max=9999.99,
                        quantiles={
                            "0.25": 45.0,
                            "0.5": 120.0,
                            "0.75": 350.0,
                            "0.95": 1250.0
                        }
                    ),
                    "email": ColumnMetric(
                        nullCount=5,
                        distinctCount=44998,
                        count=149995
                    ),
                }
            ),
            
            # Data quality assertions - test results
            "dataQualityAssertions": DataQualityAssertionsDatasetFacet(
                assertions=[
                    Assertion(
                        assertion="transaction_id_is_unique",
                        success=True,
                        column="transaction_id"
                    ),
                    Assertion(
                        assertion="amount_is_positive",
                        success=True,
                        column="amount"
                    ),
                    Assertion(
                        assertion="email_format_valid",
                        success=False,  # Found 5 invalid emails
                        column="email"
                    ),
                    Assertion(
                        assertion="no_future_dates",
                        success=True,
                        column="transaction_date"
                    ),
                ]
            ),
        }
    )
    
    # ============================================
    # OUTPUT DATASET WITH COMPREHENSIVE FACETS
    # ============================================
    
    output_dataset = OutputDataset(
        namespace="snowflake://prod-account.snowflakecomputing.com",
        name="analytics.reporting.customer_summary",
        
        # Common dataset facets
        facets={
            # Schema facet for output
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(
                        name="customer_id",
                        type="BIGINT",
                        description="Customer identifier"
                    ),
                    SchemaField(
                        name="full_name",
                        type="VARCHAR(255)",
                        description="Customer full name"
                    ),
                    SchemaField(
                        name="email_normalized",
                        type="VARCHAR(255)",
                        description="Normalized email address"
                    ),
                    SchemaField(
                        name="total_transactions",
                        type="BIGINT",
                        description="Count of transactions"
                    ),
                    SchemaField(
                        name="total_spent",
                        type="DECIMAL(12,2)",
                        description="Sum of all transaction amounts"
                    ),
                    SchemaField(
                        name="avg_transaction_value",
                        type="DECIMAL(10,2)",
                        description="Average transaction amount"
                    ),
                    SchemaField(
                        name="customer_tier",
                        type="VARCHAR(50)",
                        description="Customer value tier"
                    ),
                ]
            ),
            
            # Datasource for output
            "dataSource": DataSourceDatasetFacet(
                name="snowflake-prod",
                uri="snowflake://prod-account.snowflakecomputing.com/analytics"
            ),
            
            # Lifecycle state - what operation was performed
            "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                lifecycleStateChange="OVERWRITE",  # or CREATE, ALTER, DROP, TRUNCATE, RENAME
                previousIdentifier=None
            ),
            
            # Column lineage - track transformations
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "customer_id": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="customer_id",
                            )
                        ],
                        transformationDescription="Direct mapping from input to output",
                        transformationType="DIRECT",
                    ),
                    "email_normalized": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="email",
                            )
                        ],
                        transformationDescription="Normalized email by trimming and converting to uppercase",
                        transformationType="TRANSFORMATION",
                    ),
                    "total_transactions": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="transaction_id",
                            )
                        ],
                        transformationDescription="Count of transactions per customer",
                        transformationType="AGGREGATION",
                    ),
                    "total_spent": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="amount",
                            )
                        ],
                        transformationDescription="Total amount spent by customer",
                        transformationType="AGGREGATION",
                    ),
                    "avg_transaction_value": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="amount",
                            )
                        ],
                        transformationDescription="Average transaction value per customer",
                        transformationType="AGGREGATION",
                    ),
                    "customer_tier": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgresql://prod-db:5432",
                                name="ecommerce.public.raw_transactions",
                                field="amount",
                            )
                        ],
                        transformationDescription="Derived customer tier based on total spent",
                        transformationType="INDIRECT"
                    ),
                }
            ),
            
            # Ownership
            "ownership": OwnershipDatasetFacet(
                owners=[
                    OwnershipDatasetFacetOwners(
                        name="analytics-team",
                        type="team"
                    )
                ]
            ),
        },
        
        # Output-specific facets
        outputFacets={
            # Output statistics - results of the transformation
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=45000,  # Aggregated from 150k to 45k unique customers
                size=2621440  # 2.5 MB
            ),
        }
    )
    
    # ============================================
    # JOB FACETS
    # ============================================
    
    job = Job(
        namespace=namespace,
        name=job_name,
        facets={
            # SQL transformation logic
            "sql": SqlJobFacet(
                query="""
                INSERT INTO analytics.reporting.customer_summary
                SELECT 
                    customer_id,
                    MAX(first_name || ' ' || last_name) AS full_name,
                    UPPER(TRIM(MAX(email))) AS email_normalized,
                    COUNT(transaction_id) AS total_transactions,
                    SUM(amount) AS total_spent,
                    AVG(amount) AS avg_transaction_value,
                    CASE 
                        WHEN SUM(amount) > 10000 THEN 'platinum'
                        WHEN SUM(amount) > 5000 THEN 'gold'
                        WHEN SUM(amount) > 1000 THEN 'silver'
                        ELSE 'bronze'
                    END AS customer_tier
                FROM ecommerce.public.raw_transactions
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY customer_id
                """
            ),
            
            # Source code location
            "sourceCodeLocation": SourceCodeLocationJobFacet(
                type="git",
                url="https://github.com/company/data-pipelines"
            ),
        }
    )
    
    # ============================================
    # RUN FACETS
    # ============================================
    
    run = Run(
        runId=run_id,
        facets={
            # Nominal time - scheduled execution time
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=datetime(2025, 12, 3, 22, 0, 0, tzinfo=timezone.utc).isoformat(),
                nominalEndTime=datetime(2025, 12, 3, 22, 30, 0, tzinfo=timezone.utc).isoformat()
            ),
            
            # Parent run - if this is a task in a larger workflow
            "parent": ParentRunFacet(
                run={
                    "runId": parent_run_id
                },
                job={
                    "namespace": namespace,
                    "name": "daily_customer_analytics_dag"
                }
            ),
        }
    )
    
    # ============================================
    # CREATE AND EMIT START EVENT
    # ============================================
    
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=event_time,
        run=run,
        job=job,
        producer="https://github.com/company/data-pipelines/v2.5.0",
        inputs=[input_dataset],
        outputs=[output_dataset],
    )
    
    print("Emitting START event with comprehensive dataset facets...")
    client.emit(start_event)
    
    # Simulate processing
    import time
    time.sleep(1)
    
    # ============================================
    # CREATE AND EMIT COMPLETE EVENT
    # ============================================
    
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        producer="https://github.com/company/data-pipelines/v2.5.0",
        inputs=[input_dataset],
        outputs=[output_dataset],
    )
    
    print("Emitting COMPLETE event...")
    client.emit(complete_event)
    
    print(f"\n✓ Successfully emitted OpenLineage events with dataset facets")
    print(f"  Run ID: {run_id}")
    print(f"  Job: {namespace}/{job_name}")
    print(f"  Input: {input_dataset.name} ({input_dataset.namespace})")
    print(f"  Output: {output_dataset.name} ({output_dataset.namespace})")
    print(f"\n  Facets included:")
    print(f"    - Schema (input & output)")
    print(f"    - Data Quality Metrics (input)")
    print(f"    - Data Quality Assertions (input)")
    print(f"    - Column Lineage (output)")
    print(f"    - Datasource (input & output)")
    print(f"    - Lifecycle State (output)")
    print(f"    - Ownership (input & output)")
    print(f"    - Output Statistics (output)")
    print(f"    - SQL Job Facet")
    print(f"    - Source Code Location")
    print(f"    - Parent Run")
    print(f"    - Nominal Time")
    
    return run_id


if __name__ == "__main__":
    print("=" * 70)
    print("  OpenLineage RunEvent with Comprehensive Dataset Facets")
    print("=" * 70)
    print()
    
    run_id = create_comprehensive_lineage_event()
    
    print("\n" + "=" * 70)
    print("  Check lineage_events.jsonl for complete JSON output")
    print("=" * 70)
