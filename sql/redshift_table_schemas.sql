-- ============================================================================
-- Redshift Data Warehouse Schema Design
-- Automotive Customer Data Platform
-- ============================================================================

/*
Business Context:
- Source data comes from dealership management systems (DMS) via daily ETL
- Tables are optimized for analytical queries and marketing segmentation
- Schema supports customer 360 views and real-time marketing activation
- Encoding and distribution keys chosen for query performance at scale
*/

-- ============================================================================
-- SOURCE DATA LAYER: Raw DMS Exports
-- ============================================================================

/*
Vehicle Sales Table - Core Revenue Data
======================================
Purpose: Store all vehicle sales transactions across dealership locations
Source: Daily exports from dealership management system
Volume: ~500-1000 records per day per location
Key Business Uses:
- Revenue analysis and gross profit tracking
- Customer purchase history for marketing segmentation
- Sales team performance analysis
- Trade-in equity analysis
*/

CREATE TABLE source_data.vehicle_sales (
    -- Deal identification
    dealno character varying(256) ENCODE lzo,              -- Unique deal number
    branch character varying(255) ENCODE raw,              -- Dealership location code
    
    -- Customer information
    custno character varying(256) ENCODE lzo,              -- Customer master ID
    email1 character varying(255) ENCODE lzo,              -- Customer email
    homephone character varying(20) ENCODE lzo,            -- Customer phone
    address character varying(255) ENCODE lzo,             -- Customer address
    city character varying(100) ENCODE lzo,                -- Customer city
    state character varying(100) ENCODE lzo,               -- Customer state
    ziporpostalcode character varying(20) ENCODE lzo,      -- Customer ZIP
    
    -- Vehicle details
    vin character varying(20) ENCODE lzo,                  -- Vehicle identification number
    stockno character varying(50) ENCODE lzo,              -- Dealer stock number
    year character varying(10) ENCODE bytedict,            -- Model year
    makename character varying(100) ENCODE bytedict,       -- Manufacturer (Ford, Toyota, etc.)
    modelname character varying(256) ENCODE lzo,           -- Vehicle model
    bodystyle character varying(100) ENCODE lzo,           -- Sedan, SUV, Truck, etc.
    color character varying(100) ENCODE lzo,               -- Vehicle color
    vehiclemileage numeric(18, 2) ENCODE az64,             -- Odometer reading (used cars)
    modeltype character varying(100) ENCODE lzo,           -- New vs Used
    
    -- Financial details (critical for profitability analysis)
    saletype character varying(100) ENCODE bytedict,       -- Cash, Finance, Lease
    dealtype character varying(100) ENCODE bytedict,       -- Retail vs Wholesale
    fidealtype character varying(100) ENCODE lzo,          -- F&I deal classification
    cashprice numeric(18, 2) ENCODE az64,                  -- Base vehicle price
    costprice numeric(18, 2) ENCODE az64,                  -- Dealer cost
    outthedoorprice numeric(18, 2) ENCODE az64,           -- Total customer price
    totalgross numeric(18, 2) ENCODE az64,                 -- Total gross profit
    frontgross numeric(18, 2) ENCODE az64,                 -- Vehicle sale profit
    backgross numeric(18, 2) ENCODE az64,                  -- F&I product profit
    grossprofit numeric(18, 2) ENCODE az64,                -- Combined profit
    
    -- Customer payment details
    customercashdown numeric(18, 2) ENCODE az64,           -- Customer down payment
    totaldown numeric(18, 2) ENCODE az64,                  -- Total down (cash + trade)
    
    -- Financing information
    financesource character varying(255) ENCODE lzo,       -- Lender name
    financeamt numeric(18, 2) ENCODE az64,                 -- Amount financed
    apr numeric(18, 2) ENCODE az64,                        -- Interest rate
    term numeric(18, 0) ENCODE az64,                       -- Loan term (months)
    paymentamt numeric(18, 2) ENCODE az64,                 -- Monthly payment
    payments numeric(18, 0) ENCODE az64,                   -- Number of payments
    
    -- Trade-in details (valuable for marketing to trade-up customers)
    trade1vin character varying(20) ENCODE lzo,            -- Trade vehicle VIN
    trade1acv numeric(18, 2) ENCODE az64,                  -- Actual cash value
    trade1payoff numeric(18, 2) ENCODE az64,               -- Loan payoff amount
    trade1year character varying(10) ENCODE bytedict,      -- Trade vehicle year
    trade1makename character varying(100) ENCODE lzo,      -- Trade vehicle make
    trade1modelname character varying(256) ENCODE lzo,     -- Trade vehicle model
    trade1mileage numeric(18, 2) ENCODE az64,              -- Trade vehicle miles
    nettrade1 numeric(18, 2) ENCODE az64,                  -- Net trade equity
    totaltradeallowance numeric(18, 2) ENCODE az64,        -- Total trade value
    
    -- Lease information (when applicable)
    leasetype character varying(100) ENCODE lzo,           -- Type of lease
    leasepayment numeric(18, 2) ENCODE az64,               -- Monthly lease payment
    leasemileageallowance numeric(18, 2) ENCODE az64,      -- Annual mile allowance
    leaseendvalue numeric(18, 2) ENCODE az64,              -- Residual value
    
    -- Additional products and services
    warrantyfee numeric(18, 2) ENCODE az64,                -- Extended warranty cost
    mbicarrier character varying(100) ENCODE lzo,          -- Insurance carrier
    
    -- Sales team tracking
    crmsalesmgrname character varying(100) ENCODE lzo,     -- Sales manager
    crmsp1name character varying(100) ENCODE lzo,          -- Primary salesperson
    
    -- Audit and control fields
    salesdate timestamp without time zone ENCODE az64,     -- Sale completion date
    contractdate timestamp without time zone ENCODE az64,  -- Contract signing date
    rowlastupdatedutc timestamp without time zone ENCODE az64, -- Last modification
    accountingaccount character varying(256) ENCODE bytedict,   -- GL account
    cora_acct_code character varying(256) ENCODE lzo,      -- Additional accounting code
    weowesaletotal numeric(18, 2) ENCODE az64              -- Outstanding balance
    
) 
DISTSTYLE AUTO                          -- Let Redshift optimize distribution
SORTKEY (branch, salesdate);            -- Optimize for location + time queries

/*
Customer Master Table - Customer 360 Foundation
==============================================
Purpose: Master customer records with demographics and contact preferences
Source: Daily exports from customer management system
Volume: ~50K active customers per location
Key Business Uses:
- Customer segmentation for marketing campaigns
- Contact preference management (opt-outs, GDPR compliance)
- Demographic analysis for targeting
*/

CREATE TABLE source_data.customer_master (
    -- Customer identification
    custno character varying(256) ENCODE lzo,              -- Unique customer ID
    hostitemid character varying(256) ENCODE lzo,          -- Legacy system ID
    
    -- Personal information
    firstname character varying(100) ENCODE lzo,           -- First name
    lastname character varying(100) ENCODE lzo,            -- Last name
    middlename character varying(100) ENCODE lzo,          -- Middle name
    namesuffix character varying(20) ENCODE bytedict,      -- Jr, Sr, III, etc.
    namecompany character varying(255) ENCODE lzo,         -- Company name (B2B)
    gender character varying(10) ENCODE bytedict,          -- Gender for targeting
    birthdate date ENCODE az64,                            -- Birth date (age calc)
    
    -- Contact information
    email character varying(255) ENCODE lzo,               -- Primary email
    telephone character varying(20) ENCODE lzo,            -- Primary phone
    homephone character varying(20) ENCODE lzo,            -- Home phone
    businessphone character varying(20) ENCODE lzo,        -- Business phone
    cellular character varying(20) ENCODE lzo,             -- Cell phone
    address character varying(255) ENCODE lzo,             -- Street address
    addresssecondline character varying(255) ENCODE lzo,   -- Apt/Suite
    city character varying(100) ENCODE lzo,                -- City
    state character varying(100) ENCODE lzo,               -- State
    ziporpostalcode character varying(20) ENCODE lzo,      -- ZIP code
    county character varying(100) ENCODE lzo,              -- County
    country character varying(100) ENCODE bytedict,        -- Country
    
    -- Preferences and compliance
    contactmethod character varying(50) ENCODE bytedict,   -- Preferred contact method
    preferredlanguage character varying(50) ENCODE bytedict, -- Language preference
    blockdatashare boolean ENCODE runlength,               -- Data sharing opt-out
    blockemail boolean ENCODE runlength,                   -- Email opt-out
    blockemailnational boolean ENCODE runlength,           -- National email opt-out
    blockmail boolean ENCODE runlength,                    -- Direct mail opt-out
    blockmailnational boolean ENCODE runlength,            -- National mail opt-out
    blockphone boolean ENCODE runlength,                   -- Phone opt-out
    optoutflag boolean ENCODE runlength,                   -- General opt-out
    optoutdate date ENCODE az64,                           -- Opt-out date
    optouttime time ENCODE lzo,                            -- Opt-out time
    deletedataflag boolean ENCODE runlength,               -- GDPR deletion flag
    deletedatadate date ENCODE az64,                       -- Data deletion date
    deletedatatime time ENCODE lzo,                        -- Data deletion time
    
    -- Business information
    employer character varying(255) ENCODE lzo,            -- Employer name
    saletype character varying(100) ENCODE bytedict,       -- Customer type
    servicecustomer boolean ENCODE runlength,              -- Service-only customer
    
    -- Financial summary (for credit and targeting)
    creditlimit numeric(18, 2) ENCODE az64,                -- Credit limit
    currentdue numeric(18, 2) ENCODE az64,                 -- Current balance due
    lastpayamount numeric(18, 2) ENCODE az64,              -- Last payment amount
    lastpaydate date ENCODE az64,                          -- Last payment date
    lastpurchamount numeric(18, 2) ENCODE az64,            -- Last purchase amount
    purchasedate date ENCODE az64,                         -- Last purchase date
    lastservicedate date ENCODE az64,                      -- Last service visit
    ytdpurchases numeric(18, 2) ENCODE az64,               -- Year-to-date purchases
    totallabor numeric(18, 2) ENCODE az64,                 -- Lifetime labor charges
    totalparts numeric(18, 2) ENCODE az64,                 -- Lifetime parts charges
    totalmisc numeric(18, 2) ENCODE az64,                  -- Lifetime misc charges
    
    -- Insurance information (for F&I targeting)
    inscompany character varying(255) ENCODE lzo,          -- Insurance company
    inspolicyno character varying(100) ENCODE lzo,         -- Policy number
    inspolicyexpdate date ENCODE az64,                     -- Policy expiration
    inspolicycollisionded numeric(18, 2) ENCODE az64,      -- Collision deductible
    inspolicycomprehensiveded numeric(18, 2) ENCODE az64,  -- Comprehensive deductible
    inspolicyfiretheftded numeric(18, 2) ENCODE az64,      -- Fire/theft deductible
    
    -- Driver license information
    driverlicensestorprov character varying(100) ENCODE lzo, -- License state/province
    driverlicenseexpdate date ENCODE az64,                 -- License expiration
    
    -- Payment preferences
    paymenttype character varying(100) ENCODE bytedict,    -- Preferred payment method
    
    -- System tracking
    dateadded timestamp without time zone ENCODE az64,     -- Record creation date
    lastupdated timestamp without time zone ENCODE az64,   -- Last update date
    rowlastupdated timestamp without time zone ENCODE az64, -- Row last modified
    rowlastupdatedutc timestamp without time zone ENCODE az64, -- UTC timestamp
    deletedate timestamp without time zone ENCODE az64,    -- Deletion date
    isdeleted boolean ENCODE runlength,                    -- Soft delete flag
    
    -- Additional classification codes
    accountingaccount character varying(256) ENCODE bytedict, -- Accounting code
    cora_acct_id character varying(256) ENCODE lzo,        -- External account ID
    cora_acct_code character varying(256) ENCODE lzo       -- External account code
    
) 
DISTSTYLE AUTO                          -- Auto distribution
SORTKEY (custno, lastupdated);          -- Optimize for customer lookups and updates

/*
Service History Table - Service Revenue and Customer Retention
============================================================
Purpose: Track all service visits for warranty, maintenance, and repairs
Source: Service department management system
Volume: ~2K service visits per day per location
Key Business Uses:
- Service revenue analysis
- Customer retention and loyalty programs
- Predictive maintenance marketing
- Parts inventory planning
*/

CREATE TABLE source_data.service_history (
    -- Service identification
    serviceno character varying(256) ENCODE lzo,           -- Service order number
    custno character varying(256) ENCODE lzo,              -- Customer ID
    servicelocation character varying(255) ENCODE raw,     -- Service department code
    
    -- Vehicle information
    vin character varying(20) ENCODE lzo,                  -- Vehicle VIN
    year character varying(10) ENCODE bytedict,            -- Vehicle year
    makename character varying(100) ENCODE bytedict,       -- Vehicle make
    modelname character varying(256) ENCODE lzo,           -- Vehicle model
    mileage numeric(18, 2) ENCODE az64,                    -- Current odometer
    
    -- Service details
    servicetype character varying(100) ENCODE bytedict,    -- Warranty, Maintenance, Repair
    servicedate timestamp without time zone ENCODE az64,   -- Service date
    completiondate timestamp without time zone ENCODE az64, -- Completion date
    servicedescription text ENCODE lzo,                    -- Service description
    
    -- Financial details
    laborhours numeric(8, 2) ENCODE az64,                  -- Labor hours
    laborrate numeric(8, 2) ENCODE az64,                   -- Labor rate per hour
    laboramount numeric(18, 2) ENCODE az64,                -- Total labor cost
    partsamount numeric(18, 2) ENCODE az64,                -- Total parts cost
    miscamount numeric(18, 2) ENCODE az64,                 -- Miscellaneous charges
    totalamount numeric(18, 2) ENCODE az64,                -- Total service cost
    customerpaid numeric(18, 2) ENCODE az64,               -- Amount customer paid
    warrantyamount numeric(18, 2) ENCODE az64,             -- Warranty coverage
    
    -- Service team
    serviceadvisor character varying(100) ENCODE lzo,      -- Service advisor
    technician character varying(100) ENCODE lzo,          -- Primary technician
    
    -- System tracking
    rowlastupdatedutc timestamp without time zone ENCODE az64 -- Last modification
    
) 
DISTSTYLE AUTO
SORTKEY (servicelocation, servicedate, custno);

-- ============================================================================
-- Encoding Strategy Notes:
-- ============================================================================

/*
Redshift Encoding Optimization:
==============================

LZO: Best for high-cardinality text fields (customer names, addresses, VINs)
     - Provides good compression for unique values
     - Used for: custno, email, address, vin, stockno

BYTEDICT: Best for low-cardinality text fields (states, makes, types)
          - Extremely efficient for repeated values
          - Used for: state, makename, saletype, dealtype

AZ64: Best for numeric and date fields
      - Optimized for numeric compression and range queries
      - Used for: prices, dates, financial amounts

RAW: No compression, fastest access
     - Used for frequently filtered fields
     - Used for: branch (frequently used in WHERE clauses)

RUNLENGTH: Best for boolean and highly repetitive data
           - Efficient for flags and binary data
           - Used for: opt-out flags, boolean preferences
*/

-- ============================================================================
-- Distribution and Sort Key Strategy:
-- ============================================================================

/*
DISTSTYLE AUTO: 
- Lets Redshift choose optimal distribution based on table size and query patterns
- Best choice for most analytical workloads
- Redshift automatically redistributes as data grows

SORTKEY Rationale:
- vehicle_sales: (branch, salesdate) - Most queries filter by location and time period
- customer_master: (custno, lastupdated) - Lookups by customer, deduplication by update time
- service_history: (servicelocation, servicedate, custno) - Location + time + customer filters

These choices optimize for:
1. Location-based reporting (each dealership analyzes their own data)
2. Time-based analysis (monthly/quarterly reports)
3. Customer lookup performance (marketing segmentation queries)
4. Join performance between tables
*/
