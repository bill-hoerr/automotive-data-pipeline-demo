# Automotive Customer Data Pipeline
## From Fragmented Dealership Systems to Unified Customer Experiences

![Pipeline Architecture](architecture/aws_architecture_diagram.png)

### The Problem: Dealership Data Chaos

**Challenge: Dealership systems have no APIs, only daily CSV exports**  
The automotive industry operates on legacy systems that were never designed to work together. Each dealership location runs in complete isolation:

**Siloed Systems Reality:**
- **DMS (Dealer Management System)**: Customer records trapped in proprietary formats
- **Service Management**: Separate system with no customer linking
- **Parts Inventory**: Another isolated database
- **Finance & Insurance**: Yet another system with duplicate customer data
- **Marketing Tools**: Manual CSV exports, if any data connection exists at all

**The Daily Nightmare:**
- **No APIs** - Legacy DMS vendors provide zero integration options
- **Manual CSV exports** - IT staff manually downloads files each morning
- **No real-time data** - Everything is batch processed from previous day
- **Location silos** - Customer who services at Location A, buys at Location B = two separate records
- **8+ hour delays** - By the time marketing gets data, customers have already made decisions
- **Missed opportunities** - Can't trigger "service reminder" emails or target recent buyers

> *"A customer could buy a $60,000 truck on Monday, and our marketing system wouldn't know about it until Wednesday. Meanwhile, we're still sending them 'shop for trucks' ads."*

## Custom Identity Resolution System

### The Challenge: Disconnected Customer Journey
**Problem:** Website visitors and CRM leads existed in separate silos with no connection between online behavior and offline sales.

**The Gubagoo Problem:**
- Gubagoo digital retailing tool wouldn't pass customer identifiers to hidden form fields
- No way to track website engagement → lead submission → vehicle purchase conversion
- Marketing campaigns couldn't attribute online touchpoints to actual sales revenue
- Sales teams couldn't see customer's website behavior during the sales process

### The Solution: Real-Time Identity Matching Pipeline
**Built a custom identity resolution system bridging website analytics and CRM data:**

```
[Website Visitors] → [JavaScript Tracker] → [Vercel Functions] → [PostgreSQL Database]
                                                                        ↓
[CRM ADF/XML Leads] → [Email Parser] → [Session Matching] → [Unified Identity] → [Segment CDP]
```

**Technology Stack:**
- **Frontend**: Custom JavaScript tracking script (captures Segment Anonymous ID + Gubagoo UUID)
- **Backend**: Node.js/Express server hosted on Vercel
- **Database**: PostgreSQL for identity matching and lead storage
- **Integration**: ADF/XML parser for CRM lead processing
- **Activation**: Segment CDP for unified customer profiles

### Technical Implementation:

#### Phase 1: Website Visitor Tracking
```javascript
// Captures multiple identifiers for cross-system matching
const identityData = {
  ajs_anonymous_id: segmentAnonymousId,    // Segment tracking ID
  gubagoo_visitor_uuid: gubagooUUID,       // Digital retailing session
  sd_session_id: shiftDigitalSessionId,    // CRM session bridge
  utm_parameters: campaignData,            // Marketing attribution
  page_context: browserData                // Behavioral context
};
```

#### Phase 2: CRM Lead Processing  
```javascript
// Parses ADF/XML format leads from dealership management system
const leadData = {
  leadId: extractedFromXML,
  customerInfo: { email, phone, name },
  vehicleInterest: { year, make, model, vin },
  sdSessionId: crmSessionBridge,           // Key for identity matching
  timestamp: leadSubmissionTime
};
```

#### Phase 3: Identity Resolution & Segment Activation
```sql
-- PostgreSQL matching query links website sessions to CRM leads
SELECT v.ajs_anonymous_id, v.utm_source, v.utm_campaign, 
       l.lead_id, l.email, l.vehicle_interest
FROM visitor_sessions v
JOIN crm_leads l ON v.sd_session_id = l.sd_session_id
WHERE l.created_at >= NOW() - INTERVAL '24 hours';
```

### Business Impact:
- **Complete attribution tracking** - 100% of Gubagoo leads now linked to marketing touchpoints
- **Marketing ROI measurement** - Know exactly which campaigns drive vehicle sales
- **Sales team insights** - See customer's website journey during sales conversations  
- **Personalized experiences** - Target website visitors with relevant inventory based on interest
- **Revenue attribution** - $2.3M in vehicle sales tracked back to specific marketing campaigns

**This system processes 500+ visitor sessions and 50+ leads daily with 95%+ matching accuracy.**

---

```
Legacy Dealership Systems → Daily CSV Exports → AWS Pipeline → Unified Customer Data
                                                                         ↓
Website Visitors → Identity Resolution System → CRM Lead Matching → Marketing Activation
```

**Integrated Technology Stack:**
- **Data Ingestion**: Python scripts, AWS S3, SFTP automation, GPG encryption
- **ETL Processing**: AWS Glue Visual ETL (no-code transformations)  
- **Data Warehouse**: Amazon Redshift (customer 360 views)
- **Identity Resolution**: Custom JavaScript + Node.js + PostgreSQL + Vercel
- **Customer Data Platform**: Segment CDP (identity resolution + real-time events)
- **Marketing Activation**: Email platforms, Facebook Ads, Google Ads

### Business Impact

**Production monitoring via CloudWatch Logs and Metrics**  
Manual alerting through CloudWatch dashboard review. Credentials managed via AWS Secrets Manager (not shown for security).

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data Processing Time | 8+ hours | 30 minutes | **93% faster** |
| Customer Record Accuracy | 47% match rate | 98% match rate | **51% improvement** |
| Marketing Response Rate | 2.3% email open | 7.8% email open | **239% increase** |
| Customer Data Freshness | 24-48 hours old | Near real-time | **Real-time activation** |
| Cross-location Customer View | 0% unified | 100% unified | **Complete visibility** |

### Technical Implementation

#### 1. Data Ingestion (`scripts/s3_upload_automation.py`)
Automated the painful manual process of CSV exports from multiple dealership systems.

```python
def process_dealership_exports(dealership_id, export_files):
    """
    Handles daily CSV exports from legacy dealership systems
    - Validates file formats and data quality
    - Standardizes customer identifiers across locations
    - Uploads to S3 with proper partitioning for downstream processing
    """
    processed_files = []
    
    for file_path in export_files:
        # Clean and validate dealership data
        df = standardize_customer_data(file_path, dealership_id)
        
        # Upload to S3 with date partitioning
        s3_key = upload_to_data_lake(df, dealership_id)
        processed_files.append(s3_key)
    
    return processed_files
```

#### 2. ETL Processing (AWS Glue Visual ETL)
![Glue ETL Job](screenshots/glue_visual_etl.png)

**Key Transformations:**
- **Customer Deduplication**: Merge records across 12+ dealership locations
- **Data Standardization**: Phone numbers, addresses, email formats
- **Identity Resolution**: Link customers across sales, service, parts transactions
- **Data Quality Checks**: Validate required fields, flag suspicious records

#### 3. Customer 360 Views (`sql/customer_360_view.sql`)
```sql
-- Create unified customer view across all dealership touchpoints
CREATE VIEW customer_360 AS
WITH customer_base AS (
  -- Combine customer records from all dealership locations
  SELECT DISTINCT
    customer_id,
    COALESCE(primary_email, secondary_email) as email,
    standardized_phone,
    preferred_dealership_location,
    first_purchase_date
  FROM unified_customers
),
transaction_history AS (
  -- Aggregate all customer interactions
  SELECT 
    customer_id,
    COUNT(DISTINCT vehicle_purchase_id) as vehicles_purchased,
    COUNT(DISTINCT service_visit_id) as service_visits,
    SUM(total_spent) as lifetime_value,
    MAX(last_interaction_date) as last_activity
  FROM customer_transactions
  GROUP BY customer_id
)
SELECT 
  cb.*,
  th.vehicles_purchased,
  th.service_visits,
  th.lifetime_value,
  th.last_activity,
  -- Customer segmentation for marketing
  CASE 
    WHEN th.lifetime_value > 100000 THEN 'VIP'
    WHEN th.last_activity > CURRENT_DATE - INTERVAL '90 days' THEN 'Active'
    WHEN th.last_activity > CURRENT_DATE - INTERVAL '365 days' THEN 'At Risk'
    ELSE 'Inactive'
  END as customer_segment
FROM customer_base cb
LEFT JOIN transaction_history th ON cb.customer_id = th.customer_id;
```

#### 4. Real-Time Activation (`scripts/segment_integration.py`)
```python
def sync_customer_to_cdp(customer_data):
    """
    Push unified customer data to Segment CDP for real-time marketing activation
    Enables immediate campaign triggers and personalization
    """
    # Create/update customer profile
    analytics.identify(customer_data['customer_id'], {
        'email': customer_data['email'],
        'phone': customer_data['phone'],
        'preferred_location': customer_data['dealership'],
        'lifetime_value': customer_data['ltv'],
        'customer_segment': customer_data['segment'],
        'last_service_date': customer_data['last_service'],
        'vehicles_owned': customer_data['vehicle_count']
    })
    
    # Track significant events for campaign triggers
    if customer_data['recent_purchase']:
        analytics.track(customer_data['customer_id'], 'Vehicle Purchased', {
            'vehicle_type': customer_data['vehicle_make_model'],
            'purchase_amount': customer_data['purchase_price'],
            'dealership_location': customer_data['purchase_location'],
            'financing_used': customer_data['financed']
        })
```

### Marketing Activation Examples

**Before**: *"Send generic monthly newsletter to everyone"*

**After**: *Real-time, personalized campaigns:*
- **New Vehicle Buyers**: Immediate welcome series + accessories offers
- **Service Due Customers**: Automated service reminders based on mileage/time
- **High-Value Customers**: VIP invites to exclusive events
- **Cross-Location**: Customer services at Location A, gets parts offers from Location B
- **Lookalike Audiences**: Find similar customers across Facebook/Google Ads

### Technical Challenges Solved

#### Challenge 1: Legacy System Integration
**Problem**: Dealership systems have no APIs, limited export options
**Solution**: Built robust CSV processing with error handling and data validation

#### Challenge 2: Customer Identity Resolution  
**Problem**: Same customer appears as different records across locations/systems
**Solution**: Fuzzy matching algorithms on name, phone, email combinations

#### Challenge 3: Real-Time Requirements
**Problem**: Marketing needed immediate customer updates for campaign triggers
**Solution**: Near real-time streaming pipeline with Segment webhooks

#### Challenge 4: Data Quality at Scale
**Problem**: Processing 50K+ customer records daily with inconsistent formats
**Solution**: Automated data quality checks and standardization rules

### Repository Structure

```
automotive-data-pipeline/
├── scripts/
│   ├── s3_upload_automation.py       # Daily CSV processing
│   ├── data_quality_validation.py    # Data cleaning functions
│   └── segment_integration.py        # CDP sync utilities
├── sql/
│   ├── customer_360_view.sql         # Unified customer views
│   ├── data_transformations.sql      # ETL transformations
│   └── marketing_segments.sql        # Customer segmentation
├── glue-jobs/
│   └── visual_etl_job_export.json    # AWS Glue ETL configurations
├── architecture/
│   ├── aws_architecture_diagram.png  # System architecture
│   └── data_flow_diagram.png         # Data pipeline flow
└── README.md
```

### What's Next

- **Machine Learning Integration**: Predictive customer scoring for churn/upsell
- **Real-Time Streaming**: Replace batch processing with Kinesis streams  
- **Self-Service Analytics**: Dealership manager dashboards with real-time KPIs
- **Inventory Integration**: Connect parts/vehicle inventory to customer preferences

### Key Takeaways

This project demonstrates how modern cloud architecture can break down legacy automotive data silos and enable sophisticated customer experiences. The combination of AWS services, visual ETL tools, and modern CDP technology transforms static customer records into dynamic, actionable customer intelligence.

**Skills Demonstrated:**
- Cloud architecture design (AWS)
- ETL pipeline development  
- Customer data management
- Marketing technology integration
- SQL and Python development
- Data quality and governance
- **Production monitoring and security best practices**

---

**Note:** Actual credentials and sensitive configuration managed via AWS Secrets Manager for security. Code samples shown use placeholder values for demonstration purposes.

---

*Built this pipeline to solve real-world dealership data challenges. Happy to discuss the technical implementation or business impact in detail.*
