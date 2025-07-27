"""
AWS Glue ETL Job: Vehicle Sales Data Processing
=============================================

Purpose: Transform raw vehicle sales CSV exports from dealership DMS into 
clean, analytics-ready format in Redshift data warehouse.

Business Context:
- Vehicle sales data is the core revenue driver for automotive groups
- Raw DMS exports contain inconsistent data types and formats
- Marketing needs clean data for customer segmentation and campaign targeting
- Finance needs accurate gross profit calculations for performance analysis

This job handles:
- Data type standardization (strings → proper numeric/date types)
- Upsert logic to handle daily incremental updates
- Data quality validation for critical financial fields
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """
    Execute SQL transformations on Glue DynamicFrames
    Enables complex business logic that's difficult with visual transforms
    """
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================================
# STEP 1: Read Raw Vehicle Sales Data from S3
# ============================================================================
"""
Source: Daily CSV exports from dealership management system
Challenge: DMS systems export everything as strings, inconsistent formatting
Solution: Use Glue's built-in CSV parsing with proper quote/separator handling
"""
vehicle_sales_raw = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",          # Handle quoted fields with commas
        "withHeader": True,         # Use first row as column names
        "separator": ",",           # Standard CSV format
        "optimizePerformance": False # Ensure data quality over speed
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options={
        "paths": ["s3://automotive-data-lake/VehicleSales/"], 
        "recurse": True             # Process all daily partitions
    }, 
    transformation_ctx="vehicle_sales_raw"
)

# ============================================================================
# STEP 2: Data Cleaning and Business Logic
# ============================================================================
"""
Business Requirements:
- Extract only fields needed for analytics and marketing
- Clean customer contact information for CDP integration
- Calculate key metrics (gross profit, trade equity, etc.)
- Handle both cash sales and financed deals
"""
data_cleaning_sql = '''
SELECT 
    -- Dealership identification
    dealno,                    -- Which location made the sale
    branch,                    -- Dealership branch/department
    
    -- Customer information (for CDP sync)
    custno,                    -- Unique customer identifier
    email1,                    -- Primary email for marketing
    homephone,                 -- Contact phone number
    address,                   -- Customer address
    city,                      -- Customer city
    state,                     -- Customer state
    ziporpostalcode,          -- ZIP code for geographic analysis
    
    -- Sales team information
    crmsalesmgrname,          -- Sales manager (for commission tracking)
    crmsp1name,               -- Primary salesperson
    
    -- Vehicle details
    vin,                      -- Vehicle identification number
    stockno,                  -- Dealer stock number
    year,                     -- Model year
    makename,                 -- Manufacturer (Ford, Chevy, etc.)
    modelname,                -- Vehicle model
    bodystyle,                -- Sedan, SUV, Truck, etc.
    color,                    -- Vehicle color
    vehiclemileage,           -- Odometer reading (for used cars)
    modeltype,                -- New vs Used designation
    
    -- Financial details (critical for profitability analysis)
    saletype,                 -- Cash, Finance, Lease
    dealtype,                 -- Retail, Wholesale, etc.
    contractdate,             -- When contract was signed
    salesdate,                -- When sale was completed
    cashprice,                -- Base vehicle price
    outthedoorprice,          -- Total price customer pays
    costprice,                -- Dealer cost (for margin calculation)
    totalgross,               -- Total gross profit
    frontgross,               -- Profit from vehicle sale
    backgross,                -- Profit from F&I products
    grossprofit,              -- Total profit on deal
    
    -- Customer payments
    customercashdown,         -- Customer down payment
    totaldown,                -- Total down (cash + trade)
    
    -- Financing details (when applicable)
    financesource,            -- Lender name
    financeamt,               -- Amount financed
    apr,                      -- Interest rate
    term,                     -- Loan term in months
    paymentamt,               -- Monthly payment
    payments,                 -- Number of payments
    
    -- Trade-in information
    trade1vin,                -- Trade vehicle VIN
    trade1year,               -- Trade vehicle year
    trade1makename,           -- Trade vehicle make
    trade1modelname,          -- Trade vehicle model
    trade1mileage,            -- Trade vehicle mileage
    trade1acv,                -- Actual cash value of trade
    trade1payoff,             -- Loan payoff on trade
    nettrade1,                -- Net trade equity
    totaltradeallowance,      -- Total trade value
    
    -- Lease information (when applicable)
    leasetype,                -- Type of lease agreement
    leasepayment,             -- Monthly lease payment
    leasemileageallowance,    -- Allowed miles per year
    leaseendvalue,            -- Residual value at lease end
    
    -- Additional products sold
    warrantyfee,              -- Extended warranty cost
    
    -- System tracking
    rowlastupdatedutc,        -- When record was last modified
    mbicarrier,               -- Insurance carrier
    accountingaccount,        -- GL account code
    cora_acct_code,           -- Additional accounting code
    weowesaletotal            -- Outstanding amount owed
    
FROM myDataSource
WHERE 
    -- Data quality filters
    vin IS NOT NULL           -- Must have valid VIN
    AND custno IS NOT NULL    -- Must have customer number
    AND salesdate IS NOT NULL -- Must have sale date
'''

# Apply SQL transformations
vehicle_sales_cleaned = sparkSqlQuery(
    glueContext, 
    query=data_cleaning_sql, 
    mapping={"myDataSource": vehicle_sales_raw}, 
    transformation_ctx="vehicle_sales_cleaned"
)

# ============================================================================
# STEP 3: Data Type Conversions
# ============================================================================
"""
Challenge: DMS exports all fields as strings, even numeric/date fields
Solution: Convert to proper data types for analytics and storage efficiency

Key conversions:
- Financial amounts: string → decimal (for accurate calculations)
- Dates: string → timestamp (for time-based analysis)
- IDs: keep as strings (leading zeros matter in automotive)
"""
vehicle_sales_typed = ApplyMapping.apply(
    frame=vehicle_sales_cleaned, 
    mappings=[
        # Identifiers (keep as strings to preserve formatting)
        ("dealno", "string", "dealno", "string"),
        ("custno", "string", "custno", "string"),
        ("vin", "string", "vin", "string"),
        ("stockno", "string", "stockno", "string"),
        
        # Text fields
        ("email1", "string", "email1", "string"),
        ("homephone", "string", "homephone", "string"),
        ("address", "string", "address", "string"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("ziporpostalcode", "string", "ziporpostalcode", "string"),
        ("crmsalesmgrname", "string", "crmsalesmgrname", "string"),
        ("crmsp1name", "string", "crmsp1name", "string"),
        
        # Vehicle details
        ("year", "string", "year", "string"),
        ("makename", "string", "makename", "string"),
        ("modelname", "string", "modelname", "string"),
        ("bodystyle", "string", "bodystyle", "string"),
        ("color", "string", "color", "string"),
        ("modeltype", "string", "modeltype", "string"),
        ("saletype", "string", "saletype", "string"),
        ("dealtype", "string", "dealtype", "string"),
        
        # Financial amounts (convert to decimal for calculations)
        ("frontgross", "string", "frontgross", "decimal"),
        ("backgross", "string", "backgross", "decimal"),
        ("weowesaletotal", "string", "weowesaletotal", "decimal"),
        ("customercashdown", "string", "customercashdown", "decimal"),
        ("apr", "string", "apr", "decimal"),
        ("warrantyfee", "string", "warrantyfee", "decimal"),
        ("cashprice", "string", "cashprice", "decimal"),
        ("totalgross", "string", "totalgross", "decimal"),
        ("paymentamt", "string", "paymentamt", "decimal"),
        ("outthedoorprice", "string", "outthedoorprice", "decimal"),
        ("costprice", "string", "costprice", "decimal"),
        ("grossprofit", "string", "grossprofit", "decimal"),
        ("vehiclemileage", "string", "vehiclemileage", "decimal"),
        ("term", "string", "term", "decimal"),
        ("financeamt", "string", "financeamt", "decimal"),
        ("totaldown", "string", "totaldown", "decimal"),
        ("payments", "string", "payments", "decimal"),
        ("trade1acv", "string", "trade1acv", "decimal"),
        ("trade1payoff", "string", "trade1payoff", "decimal"),
        ("nettrade1", "string", "nettrade1", "decimal"),
        ("trade1mileage", "string", "trade1mileage", "decimal"),
        ("totaltradeallowance", "string", "totaltradeallowance", "decimal"),
        ("leasepayment", "string", "leasepayment", "decimal"),
        ("leasemileageallowance", "string", "leasemileageallowance", "decimal"),
        ("leaseendvalue", "string", "leaseendvalue", "decimal"),
        
        # Dates (convert to timestamp for date arithmetic)
        ("contractdate", "string", "contractdate", "timestamp"),
        ("salesdate", "string", "salesdate", "timestamp"),
        ("rowlastupdatedutc", "string", "rowlastupdatedutc", "timestamp"),
        
        # Trade vehicle info
        ("trade1vin", "string", "trade1vin", "string"),
        ("trade1year", "string", "trade1year", "string"),
        ("trade1makename", "string", "trade1makename", "string"),
        ("trade1modelname", "string", "trade1modelname", "string"),
        
        # Additional fields
        ("branch", "string", "branch", "string"),
        ("financesource", "string", "financesource", "string"),
        ("leasetype", "string", "leasetype", "string"),
        ("mbicarrier", "string", "mbicarrier", "string"),
        ("accountingaccount", "string", "accountingaccount", "string"),
        ("cora_acct_code", "string", "cora_acct_code", "string")
    ], 
    transformation_ctx="vehicle_sales_typed"
)

# ============================================================================
# STEP 4: Load to Redshift with Upsert Logic
# ============================================================================
"""
Business Requirement: Handle incremental daily updates
- New sales get inserted
- Modified sales get updated (DMS systems sometimes adjust deals post-sale)
- Avoid duplicates while maintaining data integrity

Upsert Strategy:
1. Create temporary table with new data
2. Delete existing records that match key fields
3. Insert all records from temp table
4. Drop temp table

Key Fields for Matching:
- dealno: Which dealership location
- custno: Customer number
- vin: Vehicle identification
- rowlastupdatedutc: When record was last modified
"""
redshift_load = glueContext.write_dynamic_frame.from_options(
    frame=vehicle_sales_typed, 
    connection_type="redshift", 
    connection_options={
        # Pre-actions: Set up target table and temp table
        "preactions": """
            CREATE TABLE IF NOT EXISTS source_data.vehicle_sales (
                dealno VARCHAR,
                mbicarrier VARCHAR,
                accountingaccount VARCHAR,
                dealtype VARCHAR,
                email1 VARCHAR,
                homephone VARCHAR,
                custno VARCHAR,
                city VARCHAR,
                state VARCHAR,
                frontgross DECIMAL,
                backgross DECIMAL,
                contractdate TIMESTAMP,
                salesdate TIMESTAMP,
                weowesaletotal DECIMAL,
                ziporpostalcode VARCHAR,
                crmsalesmgrname VARCHAR,
                crmsp1name VARCHAR,
                customercashdown DECIMAL,
                vin VARCHAR,
                address VARCHAR,
                apr DECIMAL,
                branch VARCHAR,
                warrantyfee DECIMAL,
                year VARCHAR,
                makename VARCHAR,
                modelname VARCHAR,
                cashprice DECIMAL,
                totalgross DECIMAL,
                color VARCHAR,
                paymentamt DECIMAL,
                outthedoorprice DECIMAL,
                costprice DECIMAL,
                grossprofit DECIMAL,
                saletype VARCHAR,
                trade1vin VARCHAR,
                trade1acv DECIMAL,
                trade1payoff DECIMAL,
                trade1year VARCHAR,
                nettrade1 DECIMAL,
                rowlastupdatedutc TIMESTAMP,
                stockno VARCHAR,
                bodystyle VARCHAR,
                vehiclemileage DECIMAL,
                modeltype VARCHAR,
                fidealtype VARCHAR,
                term DECIMAL,
                financesource VARCHAR,
                financeamt DECIMAL,
                totaldown DECIMAL,
                payments DECIMAL,
                trade1makename VARCHAR,
                trade1modelname VARCHAR,
                trade1mileage DECIMAL,
                totaltradeallowance DECIMAL,
                leasetype VARCHAR,
                cora_acct_code VARCHAR,
                leasepayment DECIMAL,
                leasemileageallowance DECIMAL,
                leaseendvalue DECIMAL
            );
            
            -- Create empty temp table with same structure
            DROP TABLE IF EXISTS source_data.vehicle_sales_temp;
            CREATE TABLE source_data.vehicle_sales_temp AS 
            SELECT * FROM source_data.vehicle_sales WHERE 1=2;
        """,
        
        # Post-actions: Perform upsert operation
        "postactions": """
            BEGIN;
            
            -- Delete existing records that match incoming data
            DELETE FROM source_data.vehicle_sales 
            USING source_data.vehicle_sales_temp 
            WHERE source_data.vehicle_sales_temp.dealno = source_data.vehicle_sales.dealno 
              AND source_data.vehicle_sales_temp.custno = source_data.vehicle_sales.custno 
              AND source_data.vehicle_sales_temp.vin = source_data.vehicle_sales.vin 
              AND source_data.vehicle_sales_temp.rowlastupdatedutc = source_data.vehicle_sales.rowlastupdatedutc;
            
            -- Insert all new records
            INSERT INTO source_data.vehicle_sales 
            SELECT * FROM source_data.vehicle_sales_temp;
            
            -- Clean up temp table
            DROP TABLE source_data.vehicle_sales_temp;
            
            END;
        """,
        
        # Connection settings
        "redshiftTmpDir": "s3://automotive-glue-temp/",
        "useConnectionProperties": "true",
        "dbtable": "source_data.vehicle_sales_temp",
        "connectionName": "redshift-automotive-warehouse"
    }, 
    transformation_ctx="redshift_load"
)

# Complete the job
job.commit()

"""
Job Completion Notes:
=====================

This ETL job processes vehicle sales data that enables:

1. Customer Analytics:
   - Customer lifetime value calculation
   - Purchase history tracking
   - Geographic analysis of sales

2. Marketing Activation:
   - Customer segmentation for campaigns
   - Cross-sell/upsell opportunity identification
   - Service reminder targeting

3. Business Intelligence:
   - Gross profit analysis by salesperson/location
   - Inventory turn analysis
   - F&I product penetration rates

4. Operational Reporting:
   - Daily/monthly sales reports
   - Commission calculations
   - Trade-in equity analysis

Next Steps in Pipeline:
- Customer 360 view creation
- Marketing segment definitions
- Sync to Segment CDP for activation
"""
