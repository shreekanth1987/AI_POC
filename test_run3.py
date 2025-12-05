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
    SchemaField, 
    SqlJobFacet,
    SchemaDatasetFacet,
    DataSourceDatasetFacet,
    ColumnLineageDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    DataQualityMetricsInputDatasetFacet,
    ParentRunFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    ColumnMetric
)

# Configure OpenLineage
os.environ["OPENLINEAGE_CONFIG"] = "openlineage3.yml"


class MultiLayerPipelineLineage:
    """
    Multi-layer data pipeline with OpenLineage tracking
    
    Architecture:
    1. Source → Bronze (Ingestion Layer)
    2. Bronze → Silver (Curation Layer - Cleaning & Validation)
    3. Silver → Gold (Consumption Layer - Business Aggregations)
    4. Gold → Tableau Dashboard & Python Web App
    """
    
    def __init__(self):
        self.client = OpenLineageClient.from_environment()
        self.namespace = "data_lakehouse"
        self.producer_url = "https://github.com/company/data-platform/v3.0"
        
        # Store run IDs for parent-child relationships
        self.run_ids = {
            'parent': str(uuid4()),  # Overall pipeline run
            'bronze': str(uuid4()),   # Layer 1: Source to Bronze
            'silver': str(uuid4()),   # Layer 2: Bronze to Silver
            'gold': str(uuid4()),     # Layer 3: Silver to Gold
            'tableau': str(uuid4()),  # Layer 4a: Gold to Tableau
            'webapp': str(uuid4()),   # Layer 4b: Gold to Web App
        }
    
    # =========================================================================
    # LAYER 1: SOURCE TO BRONZE (INGESTION)
    # =========================================================================
    
    def emit_source_to_bronze_lineage(self):
        """
        Layer 1: Raw data ingestion from source systems to Bronze layer
        """
        print("\n" + "=" * 70)
        print("LAYER 1: Source → Bronze (Ingestion)")
        print("=" * 70)
        
        job_name = "source_to_bronze_ingestion"
        run_id = self.run_ids['bronze']
        
        # Input: Source database
        input_dataset = InputDataset(
            namespace="mysql://prod-db.company.com:3306",
            name="ecommerce.orders",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="order_id", type="BIGINT"),
                        SchemaField(name="customer_id", type="BIGINT"),
                        SchemaField(name="product_id", type="BIGINT"),
                        SchemaField(name="order_date", type="TIMESTAMP"),
                        SchemaField(name="amount", type="DECIMAL(10,2)"),
                        SchemaField(name="status", type="VARCHAR(50)"),
                        SchemaField(name="shipping_address", type="TEXT"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="mysql-prod-01",
                    uri="mysql://prod-db.company.com:3306/ecommerce"
                ),
            },
            inputFacets={
                "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                    rowCount=250000,
                    bytes=104857600,  # 100 MB
                )
            }
        )
        
        # Output: Bronze layer (raw data lake)
        output_dataset = OutputDataset(
            namespace="s3://company-datalake",
            name="bronze/orders/raw",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="order_id", type="BIGINT"),
                        SchemaField(name="customer_id", type="BIGINT"),
                        SchemaField(name="product_id", type="BIGINT"),
                        SchemaField(name="order_date", type="TIMESTAMP"),
                        SchemaField(name="amount", type="DECIMAL(10,2)"),
                        SchemaField(name="status", type="VARCHAR(50)"),
                        SchemaField(name="shipping_address", type="TEXT"),
                        SchemaField(name="_ingestion_timestamp", type="TIMESTAMP"),
                        SchemaField(name="_source_file", type="VARCHAR(255)"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="s3-datalake-bronze",
                    uri="s3://company-datalake/bronze"
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "order_id": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="mysql://prod-db.company.com:3306",
                                    name="ecommerce.orders",
                                    field="order_id",
                                )
                            ],
                            transformationDescription="Direct copy from source to Bronze layer",
                            transformationType="IDENTITY"
                        ),
                        # Similar for other columns (abbreviated for brevity)
                    }
                )
            },
            outputFacets={
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=250000,
                    size=104857600
                )
            }
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={
                "sql": SqlJobFacet(
                    query="""
                    -- Incremental ingestion from source
                    COPY INTO bronze.orders
                    FROM (
                        SELECT *, 
                               CURRENT_TIMESTAMP() as _ingestion_timestamp,
                               'mysql-prod' as _source_file
                        FROM mysql.ecommerce.orders
                        WHERE order_date >= CURRENT_DATE - INTERVAL 1 DAY
                    )
                    FILE_FORMAT = (TYPE = PARQUET)
                    """
                )
            }
        )
        
        run = Run(
            runId=run_id,
            facets={
                "parent": ParentRunFacet(
                    run={"runId": self.run_ids['parent']},
                    job={
                        "namespace": self.namespace,
                        "name": "daily_pipeline_orchestration"
                    }
                )
            }
        )
        
        # Emit START and COMPLETE events
        self._emit_events(RunState.START, run, job, [input_dataset], [output_dataset])
        print(f"✓ Ingested 250,000 records to Bronze layer")
        self._emit_events(RunState.COMPLETE, run, job, [input_dataset], [output_dataset])
        
        return output_dataset
    
    # =========================================================================
    # LAYER 2: BRONZE TO SILVER (CURATION)
    # =========================================================================
    
    def emit_bronze_to_silver_lineage(self, bronze_dataset):
        """
        Layer 2: Data cleaning, validation, and standardization
        """
        print("\n" + "=" * 70)
        print("LAYER 2: Bronze → Silver (Curation)")
        print("=" * 70)
        
        job_name = "bronze_to_silver_curation"
        run_id = self.run_ids['silver']
        
        # Input: Bronze layer
        input_dataset = InputDataset(
            namespace=bronze_dataset.namespace,
            name=bronze_dataset.name,
            facets=bronze_dataset.facets,
            inputFacets={
                "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                    rowCount=250000,
                    bytes=104857600,
                )
            }
        )
        
        # Output: Silver layer (cleaned and validated)
        output_dataset = OutputDataset(
            namespace="s3://company-datalake",
            name="silver/orders/validated",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="order_id", type="BIGINT"),
                        SchemaField(name="customer_id", type="BIGINT"),
                        SchemaField(name="product_id", type="BIGINT"),
                        SchemaField(name="order_date", type="DATE"),
                        SchemaField(name="order_timestamp", type="TIMESTAMP"),
                        SchemaField(name="amount", type="DECIMAL(10,2)"),
                        SchemaField(name="status_normalized", type="VARCHAR(50)"),
                        SchemaField(name="shipping_country", type="VARCHAR(100)"),
                        SchemaField(name="is_valid", type="BOOLEAN"),
                        SchemaField(name="data_quality_score", type="DECIMAL(3,2)"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="s3-datalake-silver",
                    uri="s3://company-datalake/silver"
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "order_id": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="order_id",
                                )
                            ],
                            transformationDescription="Validated for non-null and positive values",
                            transformationType="VALIDATION"
                        ),
                        "order_date": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="order_date",
                                )
                            ],
                            transformationDescription="Converted from TIMESTAMP to DATE",
                            transformationType="TRANSFORMATION"
                        ),
                        "status_normalized": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="status",
                                )
                            ],
                            transformationDescription="Standardized status values to uppercase and trimmed whitespace",
                            transformationType="TRANSFORMATION"
                        ),
                        "shipping_country": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="shipping_address",
                                )
                            ],
                            transformationDescription="Extracted country from shipping address",
                            transformationType="TRANSFORMATION"
                        ),
                        "is_valid": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="order_id",
                                ),
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="bronze/orders/raw",
                                    field="amount",
                                )
                            ],
                            transformationDescription="Flagged records as valid or invalid based on business rules",
                            transformationType="VALIDATION"
                        ),
                    }
                )
            },
            outputFacets={
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=248500,  # 1500 invalid records removed
                    size=98304000
                )
            }
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={
                "sql": SqlJobFacet(
                    query="""
                    -- Data cleansing and validation
                    INSERT INTO silver.orders
                    SELECT 
                        order_id,
                        customer_id,
                        product_id,
                        CAST(order_date AS DATE) as order_date,
                        order_date as order_timestamp,
                        amount,
                        UPPER(TRIM(status)) as status_normalized,
                        EXTRACT_COUNTRY(shipping_address) as shipping_country,
                        CASE 
                            WHEN order_id IS NOT NULL 
                            AND customer_id IS NOT NULL
                            AND amount > 0
                            AND status IN ('PENDING', 'COMPLETED', 'CANCELLED')
                            THEN TRUE 
                            ELSE FALSE 
                        END as is_valid,
                        CALCULATE_QUALITY_SCORE(*) as data_quality_score
                    FROM bronze.orders
                    WHERE _ingestion_timestamp >= CURRENT_DATE - INTERVAL 1 DAY
                    """
                )
            }
        )
        
        run = Run(
            runId=run_id,
            facets={
                "parent": ParentRunFacet(
                    run={"runId": self.run_ids['parent']},
                    job={
                        "namespace": self.namespace,
                        "name": "daily_pipeline_orchestration"
                    }
                )
            }
        )
        
        self._emit_events(RunState.START, run, job, [input_dataset], [output_dataset])
        print(f"✓ Cleaned and validated 248,500 records in Silver layer")
        print(f"  - Removed 1,500 invalid records")
        print(f"  - Standardized status values")
        print(f"  - Extracted country from addresses")
        self._emit_events(RunState.COMPLETE, run, job, [input_dataset], [output_dataset])
        
        return output_dataset
    
    # =========================================================================
    # LAYER 3: SILVER TO GOLD (CONSUMPTION LAYER)
    # =========================================================================
    
    def emit_silver_to_gold_lineage(self, silver_dataset):
        """
        Layer 3: Business aggregations and enrichments for analytics
        """
        print("\n" + "=" * 70)
        print("LAYER 3: Silver → Gold (Consumption Layer)")
        print("=" * 70)
        
        job_name = "silver_to_gold_aggregation"
        run_id = self.run_ids['gold']
        
        # Input: Silver layer
        input_dataset = InputDataset(
            namespace=silver_dataset.namespace,
            name=silver_dataset.name,
            facets=silver_dataset.facets,
            inputFacets={
                "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                    rowCount=248500,
                )
            }
        )
        
        # Output: Gold layer (business-ready aggregations)
        output_dataset = OutputDataset(
            namespace="snowflake://prod.snowflakecomputing.com",
            name="analytics.gold.customer_order_summary",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="customer_id", type="BIGINT"),
                        SchemaField(name="order_month", type="DATE"),
                        SchemaField(name="total_orders", type="BIGINT"),
                        SchemaField(name="total_revenue", type="DECIMAL(12,2)"),
                        SchemaField(name="avg_order_value", type="DECIMAL(10,2)"),
                        SchemaField(name="completed_orders", type="BIGINT"),
                        SchemaField(name="cancelled_orders", type="BIGINT"),
                        SchemaField(name="customer_tier", type="VARCHAR(50)"),
                        SchemaField(name="top_shipping_country", type="VARCHAR(100)"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="snowflake-prod-warehouse",
                    uri="snowflake://prod.snowflakecomputing.com/analytics"
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "customer_id": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="customer_id",
                                )
                            ],
                            transformationDescription="Direct copy from Silver to Gold layer",
                            transformationType="IDENTITY"
                        ),
                        "order_month": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="order_date",
                                )
                            ],
                            transformationDescription="Truncated order_date to month level",
                            transformationType="TRANSFORMATION"
                        ),
                        "total_orders": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="order_id",
                                )
                            ],
                            transformationDescription="Count of orders per customer per month",
                            transformationType="AGGREGATION"
                        ),
                        "total_revenue": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="amount",
                                )
                            ],
                            transformationDescription="Sum of order amounts per customer per month",
                            transformationType="AGGREGATION"
                        ),
                        "avg_order_value": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="amount",
                                )
                            ],
                            transformationDescription="Average order value per customer per month",
                            transformationType="AGGREGATION"
                        ),
                        "completed_orders": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="status_normalized",
                                )
                            ],
                            transformationDescription="Count of completed orders per customer per month",
                            transformationType="AGGREGATION"
                        ),
                        "customer_tier": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="s3://company-datalake",
                                    name="silver/orders/validated",
                                    field="amount",
                                )
                            ],
                            transformationDescription="Derived customer tier based on total revenue",
                            transformationType="DERIVATION"
                        ),
                    }
                )
            },
            outputFacets={
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=52000,  # Aggregated to customer-month level
                    size=5242880
                )
            }
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={
                "sql": SqlJobFacet(
                    query="""
                    -- Business aggregations for analytics
                    INSERT INTO analytics.gold.customer_order_summary
                    SELECT 
                        customer_id,
                        DATE_TRUNC('MONTH', order_date) as order_month,
                        COUNT(order_id) as total_orders,
                        SUM(amount) as total_revenue,
                        AVG(amount) as avg_order_value,
                        COUNT_IF(status_normalized = 'COMPLETED') as completed_orders,
                        COUNT_IF(status_normalized = 'CANCELLED') as cancelled_orders,
                        CASE 
                            WHEN SUM(amount) > 10000 THEN 'platinum'
                            WHEN SUM(amount) > 5000 THEN 'gold'
                            WHEN SUM(amount) > 1000 THEN 'silver'
                            ELSE 'bronze'
                        END as customer_tier,
                        MODE(shipping_country) as top_shipping_country
                    FROM silver.orders
                    WHERE is_valid = TRUE
                    GROUP BY customer_id, DATE_TRUNC('MONTH', order_date)
                    """
                )
            }
        )
        
        run = Run(
            runId=run_id,
            facets={
                "parent": ParentRunFacet(
                    run={"runId": self.run_ids['parent']},
                    job={
                        "namespace": self.namespace,
                        "name": "daily_pipeline_orchestration"
                    }
                )
            }
        )
        
        self._emit_events(RunState.START, run, job, [input_dataset], [output_dataset])
        print(f"✓ Created 52,000 aggregated records in Gold layer")
        print(f"  - Customer-month level aggregations")
        print(f"  - Calculated customer tiers")
        print(f"  - Ready for BI consumption")
        self._emit_events(RunState.COMPLETE, run, job, [input_dataset], [output_dataset])
        
        return output_dataset
    
    # =========================================================================
    # LAYER 4A: GOLD TO TABLEAU DASHBOARD
    # =========================================================================
    
    def emit_gold_to_tableau_lineage(self, gold_dataset):
        """
        Layer 4a: Consumption by Tableau dashboard
        """
        print("\n" + "=" * 70)
        print("LAYER 4A: Gold → Tableau Dashboard")
        print("=" * 70)
        
        job_name = "tableau_dashboard_refresh"
        run_id = self.run_ids['tableau']
        
        # Input: Gold layer
        input_dataset = InputDataset(
            namespace=gold_dataset.namespace,
            name=gold_dataset.name,
            facets=gold_dataset.facets,
        )
        
        # Output: Tableau workbook/dashboard
        output_dataset = OutputDataset(
            namespace="tableau://prod-server.company.com",
            name="Sales_Analytics/Customer_Performance_Dashboard",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="Customer Segment", type="STRING", 
                                  description="Derived from customer_tier"),
                        SchemaField(name="Monthly Revenue", type="DOUBLE",
                                  description="Aggregated from total_revenue"),
                        SchemaField(name="Order Volume", type="INTEGER",
                                  description="Sum of total_orders"),
                        SchemaField(name="Average Order Value", type="DOUBLE",
                                  description="From avg_order_value"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="tableau-prod-server",
                    uri="tableau://prod-server.company.com/Sales_Analytics"
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "Customer Segment": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="snowflake://prod.snowflakecomputing.com",
                                    name="analytics.gold.customer_order_summary",
                                    field="customer_tier",
                                )
                            ],
                            transformationDescription="Mapped customer_tier to Tableau dimension",
                            transformationType="DERIVATION"
                        ),
                        "Monthly Revenue": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="snowflake://prod.snowflakecomputing.com",
                                    name="analytics.gold.customer_order_summary",
                                    field="total_revenue",
                                )
                            ],
                            transformationDescription="Aggregated total_revenue for Tableau visualization",
                            transformationType="AGGREGATION"
                        ),
                    }
                )
            },
            outputFacets={
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=52000,  # Same as input (pass-through with visualizations)
                )
            }
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={
                "sql": SqlJobFacet(
                    query="""
                    -- Tableau data extract refresh
                    -- Dashboard: Customer Performance Dashboard
                    -- Visualizations: Revenue trends, Customer segmentation, Geographic analysis
                    -- Filters: Date range, Customer tier, Country
                    """
                )
            }
        )
        
        run = Run(
            runId=run_id,
            facets={
                "parent": ParentRunFacet(
                    run={"runId": self.run_ids['parent']},
                    job={
                        "namespace": self.namespace,
                        "name": "daily_pipeline_orchestration"
                    }
                )
            }
        )
        
        self._emit_events(RunState.START, run, job, [input_dataset], [output_dataset])
        print(f"✓ Refreshed Tableau dashboard")
        print(f"  - Dashboard: Customer Performance Dashboard")
        print(f"  - Workbook: Sales_Analytics")
        print(f"  - Data source: Snowflake connection")
        self._emit_events(RunState.COMPLETE, run, job, [input_dataset], [output_dataset])
    
    # =========================================================================
    # LAYER 4B: GOLD TO PYTHON WEB APP
    # =========================================================================
    
    def emit_gold_to_webapp_lineage(self, gold_dataset):
        """
        Layer 4b: Consumption by Python web application (FastAPI/Flask)
        """
        print("\n" + "=" * 70)
        print("LAYER 4B: Gold → Python Web Application")
        print("=" * 70)
        
        job_name = "webapp_api_data_refresh"
        run_id = self.run_ids['webapp']
        
        # Input: Gold layer
        input_dataset = InputDataset(
            namespace=gold_dataset.namespace,
            name=gold_dataset.name,
            facets=gold_dataset.facets,
        )
        
        # Output: Web app cache/API layer
        output_dataset = OutputDataset(
            namespace="redis://cache.company.com:6379",
            name="webapp_cache/customer_summary_api",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="customer_id", type="BIGINT"),
                        SchemaField(name="metrics", type="JSON",
                                  description="JSON containing all aggregated metrics"),
                        SchemaField(name="last_updated", type="TIMESTAMP"),
                        SchemaField(name="cache_key", type="STRING"),
                    ]
                ),
                "dataSource": DataSourceDatasetFacet(
                    name="redis-cache-prod",
                    uri="redis://cache.company.com:6379/0"
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "customer_id": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="snowflake://prod.snowflakecomputing.com",
                                    name="analytics.gold.customer_order_summary",
                                    field="customer_id",
                                )
                            ],
                            transformationDescription="Direct mapping from Gold layer to API cache",
                            transformationType="IDENTITY"
                        ),
                        "metrics": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="snowflake://prod.snowflakecomputing.com",
                                    name="analytics.gold.customer_order_summary",
                                    field="total_orders"
                                ),
                                ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace="snowflake://prod.snowflakecomputing.com",
                                    name="analytics.gold.customer_order_summary",
                                    field="total_revenue"
                                ),
                            ],
                            transformationDescription="Aggregated metrics serialized into JSON for API response",
                            transformationType="DERIVATION"
                        ),
                    }
                )
            },
            outputFacets={
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=52000,
                    size=10485760  # 10 MB in Redis cache
                )
            }
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={
                "sql": SqlJobFacet(
                    query="""
                    -- Python web app data refresh
                    -- FastAPI endpoint: /api/v1/customers/{customer_id}/summary
                    -- Cache strategy: Redis with 1-hour TTL
                    SELECT 
                        customer_id,
                        OBJECT_CONSTRUCT(
                            'total_orders', total_orders,
                            'total_revenue', total_revenue,
                            'avg_order_value', avg_order_value,
                            'customer_tier', customer_tier,
                            'completed_orders', completed_orders
                        ) as metrics
                    FROM analytics.gold.customer_order_summary
                    """
                )
            }
        )
        
        run = Run(
            runId=run_id,
            facets={
                "parent": ParentRunFacet(
                    run={"runId": self.run_ids['parent']},
                    job={
                        "namespace": self.namespace,
                        "name": "daily_pipeline_orchestration"
                    }
                )
            }
        )
        
        self._emit_events(RunState.START, run, job, [input_dataset], [output_dataset])
        print(f"✓ Refreshed Python web app data cache")
        print(f"  - Application: Customer Analytics API (FastAPI)")
        print(f"  - Cache: Redis (1-hour TTL)")
        print(f"  - API Endpoints: /api/v1/customers/*/summary")
        print(f"  - Frontend: React dashboard consuming API")
        self._emit_events(RunState.COMPLETE, run, job, [input_dataset], [output_dataset])
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _emit_events(self, event_type, run, job, inputs, outputs):
        """Emit START or COMPLETE event"""
        event = RunEvent(
            eventType=event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job,
            producer=self.producer_url,
            inputs=inputs,
            outputs=outputs,
        )
        self.client.emit(event)
    
    def run_complete_pipeline(self):
        """Execute the complete multi-layer pipeline with lineage tracking"""
        print("\n" + "=" * 70)
        print("  MULTI-LAYER DATA PIPELINE WITH OPENLINEAGE")
        print("=" * 70)
        print(f"\nParent Pipeline Run ID: {self.run_ids['parent']}")
        
        # Layer 1: Source → Bronze
        bronze_dataset = self.emit_source_to_bronze_lineage()
        
        # Layer 2: Bronze → Silver
        silver_dataset = self.emit_bronze_to_silver_lineage(bronze_dataset)
        
        # Layer 3: Silver → Gold
        gold_dataset = self.emit_silver_to_gold_lineage(silver_dataset)
        
        # Layer 4a: Gold → Tableau
        self.emit_gold_to_tableau_lineage(gold_dataset)
        
        # Layer 4b: Gold → Python Web App
        self.emit_gold_to_webapp_lineage(gold_dataset)
        
        print("\n" + "=" * 70)
        print("  ✓ PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nData Flow Summary:")
        print("  MySQL Source")
        print("    ↓ (250,000 records)")
        print("  Bronze Layer (S3)")
        print("    ↓ (248,500 records - cleaned)")
        print("  Silver Layer (S3)")
        print("    ↓ (52,000 records - aggregated)")
        print("  Gold Layer (Snowflake)")
        print("    ├→ Tableau Dashboard (Customer Performance)")
        print("    └→ Python Web App API (Customer Summary)")
        print("\n✓ All lineage events written to: lineage_events.jsonl")
        print("=" * 70)


if __name__ == "__main__":
    # Run the complete multi-layer pipeline
    pipeline = MultiLayerPipelineLineage()
    pipeline.run_complete_pipeline()
