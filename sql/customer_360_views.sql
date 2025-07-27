-- ============================================================================
-- Customer 360 Views - Marketing Intelligence Layer
-- Automotive Customer Data Platform
-- ============================================================================

/*
Business Purpose:
These views transform raw dealership data into marketing-ready customer profiles
and enable real-time segmentation for campaigns via Segment CDP integration.

Key Capabilities:
- Clean, validated customer profiles with marketing preferences
- Complete purchase and service history
- Customer segmentation and lifetime value calculation
- GDPR/CCPA compliance with opt-out handling
- Real-time sync capability for marketing activation
*/

-- ============================================================================
-- Clean Customer Profiles - Foundation for All Marketing
-- ============================================================================

CREATE OR REPLACE VIEW marketing.customer_profiles AS
WITH deduplicated_customers AS (
    /*
    Challenge: DMS systems create duplicate customer records when:
    - Customer moves and updates address
    - Different spellings of name are entered
    - Multiple family members use same contact info
    
    Solution: Use row_number() to get most recent record per customer
    */
    SELECT 
        custno,
        firstname,
        lastname,
        email,
        telephone,
        address,
        addresssecondline,
        city,
        state,
        ziporpostalcode,
        gender,
        birthdate,
        preferredlanguage,
        
        -- Contact preferences (GDPR compliance)
        blockdatashare,
        blockemail,
        blockemailnational,
        blockmail,
        blockmailnational,
        blockphone,
        optoutflag,
        optoutdate,
        deletedataflag,
        deletedatadate,
        
        -- Customer value indicators
        lastservicedate,
        ytdpurchases,
        totallabor,
        totalparts,
        
        -- System fields
        lastupdated,
        dateadded,
        
        -- Deduplication: Get most recent record per customer
        ROW_NUMBER() OVER (
            PARTITION BY custno 
            ORDER BY lastupdated DESC
        ) AS customer_rank
        
    FROM source_data.customer_master
    WHERE 
        -- Exclude deleted and invalid records
        (isdeleted IS NULL OR isdeleted = false)
        AND (deletedataflag IS NULL OR deletedataflag = false)
)

SELECT 
    -- Standardized customer identifier for CDP sync
    CONCAT('AUTO-', custno) AS user_id,
    
    -- Core customer information
    custno,
    firstname,
    lastname,
    
    -- Contact information (cleaned and validated)
    CASE 
        WHEN email IS NOT NULL 
             AND email != '' 
             AND email !~* '^(no|nothanks|none|noemail|no.email|na)@'  -- Exclude dummy emails
             AND email !~* '(@example.com|@test.com|@invalid.com)'      -- Exclude test emails
             AND email ~* '@.+\..+'                                     -- Must have @ and domain
        THEN LOWER(TRIM(email))
        ELSE NULL 
    END AS email,
    
    CASE 
        WHEN telephone IS NOT NULL 
             AND telephone != '' 
             AND LENGTH(REGEXP_REPLACE(telephone, '[^0-9]', '')) >= 10  -- At least 10 digits
        THEN REGEXP_REPLACE(telephone, '[^0-9]', '')                    -- Clean to digits only
        ELSE NULL 
    END AS phone,
    
    -- Address information
    TRIM(address) AS address,
    TRIM(addresssecondline) AS address2,
    TRIM(city) AS city,
    UPPER(TRIM(state)) AS state,
    REGEXP_REPLACE(ziporpostalcode, '[^0-9-]', '') AS zip,
    
    -- Demographics
    CASE 
        WHEN gender IN ('M', 'Male', 'MALE', 'm') THEN 'Male'
        WHEN gender IN ('F', 'Female', 'FEMALE', 'f') THEN 'Female'
        ELSE 'Unknown'
    END AS gender,
    
    birthdate,
    
    CASE 
        WHEN birthdate IS NOT NULL 
        THEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate)
        ELSE NULL 
    END AS age,
    
    CASE 
        WHEN birthdate IS NOT NULL THEN
            CASE 
                WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) < 25 THEN 'Gen Z (Under 25)'
                WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) < 40 THEN 'Millennial (25-39)'
                WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) < 55 THEN 'Gen X (40-54)'
                WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birthdate) < 75 THEN 'Boomer (55-74)'
                ELSE 'Silent (75+)'
            END
        ELSE 'Unknown'
    END AS generation,
    
    -- Language preference for campaign localization
    COALESCE(preferredlanguage, 'English') AS preferred_language,
    
    -- Marketing preferences and compliance flags
    COALESCE(blockemail, false) AS email_opt_out,
    COALESCE(blockphone, false) AS phone_opt_out,
    COALESCE(blockmail, false) AS mail_opt_out,
    COALESCE(optoutflag, false) AS marketing_opt_out,
    optoutdate AS opt_out_date,
    
    -- Customer value metrics
    COALESCE(ytdpurchases, 0) AS ytd_purchases,
    COALESCE(totallabor + totalparts, 0) AS lifetime_service_value,
    lastservicedate AS last_service_date,
    
    -- Recency indicators for campaign targeting
    CASE 
        WHEN lastservicedate IS NULL THEN 'Never Serviced'
        WHEN lastservicedate >= CURRENT_DATE - INTERVAL '90 days' THEN 'Recent (0-90 days)'
        WHEN lastservicedate >= CURRENT_DATE - INTERVAL '180 days' THEN 'Moderate (91-180 days)'
        WHEN lastservicedate >= CURRENT_DATE - INTERVAL '365 days' THEN 'Distant (181-365 days)'
        ELSE 'Inactive (365+ days)'
    END AS service_recency,
    
    -- Account lifecycle
    dateadded AS customer_since,
    EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM dateadded) AS customer_tenure_years,
    lastupdated AS last_updated

FROM deduplicated_customers
WHERE 
    customer_rank = 1                           -- Only most recent record per customer
    AND firstname IS NOT NULL                   -- Must have first name
    AND firstname != ''                         -- Must not be empty
    AND lastname IS NOT NULL                    -- Must have last name  
    AND lastname != ''                          -- Must not be empty
    AND (
        -- Must have valid email OR phone for marketing contact
        (email IS NOT NULL AND email != '' AND email ~* '@.+\..+') 
        OR 
        (telephone IS NOT NULL AND telephone != '' AND LENGTH(REGEXP_REPLACE(telephone, '[^0-9]', '')) >= 10)
    );

-- ============================================================================
-- Vehicle Purchase History - Customer Relationship Timeline
-- ============================================================================

CREATE OR REPLACE VIEW marketing.customer_vehicle_history AS
WITH ranked_sales AS (
    /*
    Business Logic: Handle deal modifications and duplicates
    - Sometimes deals are modified after initial entry
    - Use most recent record per deal for accurate data
    - Include both new and used vehicle sales
    */
    SELECT 
        custno,
        dealno,
        vin,
        stockno,
        makename,
        modelname,
        year,
        bodystyle,
        color,
        modeltype,                              -- New vs Used
        vehiclemileage,
        saletype,                               -- Cash, Finance, Lease
        dealtype,                               -- Retail vs Wholesale
        branch,                                 -- Dealership location
        
        -- Financial details
        cashprice,
        costprice,
        outthedoorprice,
        totalgross,
        frontgross,
        backgross,
        
        -- Customer payment information
        customercashdown,
        totaldown,
        
        -- Financing details
        financesource,
        financeamt,
        apr,
        term,
        paymentamt,
        
        -- Trade information
        trade1vin,
        trade1acv,
        trade1payoff,
        nettrade1,
        trade1year,
        trade1makename,
        trade1modelname,
        
        -- Lease details
        leasetype,
        leasepayment,
        leasemileageallowance,
        leaseendvalue,
        
        -- Sales team
        crmsalesmgrname AS sales_manager,
        crmsp1name AS salesperson,
        
        -- Important dates
        contractdate,
        salesdate,
        
        -- System tracking
        rowlastupdatedutc,
        
        -- Ranking for deduplication
        ROW_NUMBER() OVER (
            PARTITION BY dealno 
            ORDER BY rowlastupdatedutc DESC
        ) AS deal_rank
        
    FROM source_data.vehicle_sales
    WHERE 
        custno IS NOT NULL
        AND vin IS NOT NULL
        AND contractdate IS NOT NULL
)

SELECT 
    -- Customer linkage
    CONCAT('AUTO-', custno) AS user_id,
    custno,
    
    -- Deal identification
    dealno AS deal_number,
    vin,
    stockno AS stock_number,
    
    -- Vehicle details
    makename AS make,
    modelname AS model,
    year,
    bodystyle AS body_style,
    color,
    CASE 
        WHEN modeltype ILIKE '%new%' THEN 'New'
        WHEN modeltype ILIKE '%used%' THEN 'Used'
        WHEN modeltype ILIKE '%certified%' THEN 'Certified Pre-Owned'
        ELSE modeltype
    END AS vehicle_condition,
    vehiclemileage AS odometer_reading,
    
    -- Transaction type classification
    CASE 
        WHEN saletype ILIKE '%cash%' THEN 'Cash'
        WHEN saletype ILIKE '%finance%' THEN 'Financed'
        WHEN saletype ILIKE '%lease%' THEN 'Lease'
        ELSE saletype
    END AS transaction_type,
    
    dealtype AS deal_category,
    branch AS dealership_location,
    
    -- Financial metrics
    cashprice AS vehicle_price,
    outthedoorprice AS total_price,
    costprice AS dealer_cost,
    totalgross AS total_gross_profit,
    frontgross AS vehicle_gross_profit,
    backgross AS finance_insurance_profit,
    
    -- Customer investment
    customercashdown AS cash_down,
    totaldown AS total_down_payment,
    
    -- Financing terms (when applicable)
    financesource AS lender,
    financeamt AS amount_financed,
    apr AS interest_rate,
    term AS finance_term_months,
    paymentamt AS monthly_payment,
    
    -- Trade-in analysis (valuable for trade-up marketing)
    trade1vin AS trade_vin,
    trade1acv AS trade_actual_value,
    trade1payoff AS trade_payoff,
    nettrade1 AS trade_equity,
    CONCAT(trade1year, ' ', trade1makename, ' ', trade1modelname) AS trade_vehicle_description,
    
    -- Lease details (when applicable)
    leasetype AS lease_type,
    leasepayment AS lease_payment,
    leasemileageallowance AS lease_mile_allowance,
    leaseendvalue AS lease_residual_value,
    
    -- Sales team for attribution
    sales_manager,
    salesperson,
    
    -- Timeline
    contractdate AS purchase_date,
    salesdate AS delivery_date,
    
    -- Purchase sequence for customer
    ROW_NUMBER() OVER (
        PARTITION BY custno 
        ORDER BY contractdate ASC
    ) AS purchase_sequence,
    
    -- Recency for targeting
    CURRENT_DATE - contractdate::date AS days_since_purchase,
    
    CASE 
        WHEN contractdate >= CURRENT_DATE - INTERVAL '30 days' THEN 'Recent (0-30 days)'
        WHEN contractdate >= CURRENT_DATE - INTERVAL '90 days' THEN 'Moderate (31-90 days)' 
        WHEN contractdate >= CURRENT_DATE - INTERVAL '365 days' THEN 'Past Year (91-365 days)'
        WHEN contractdate >= CURRENT_DATE - INTERVAL '1095 days' THEN 'Historical (1-3 years)'
        ELSE 'Legacy (3+ years)'
    END AS purchase_recency,
    
    rowlastupdatedutc AS last_updated

FROM ranked_sales
WHERE deal_rank = 1                             -- Most recent version of each deal
ORDER BY custno, contractdate DESC;

-- ============================================================================
-- Customer Lifetime Value & Segmentation
-- ============================================================================

CREATE OR REPLACE VIEW marketing.customer_segments AS
WITH customer_metrics AS (
    /*
    Calculate comprehensive customer value metrics
    Combines purchase history, service revenue, and behavioral patterns
    */
    SELECT 
        cp.user_id,
        cp.custno,
        cp.firstname,
        cp.lastname,
        cp.email,
        cp.phone,
        cp.state,
        cp.zip,
        cp.age,
        cp.generation,
        cp.customer_tenure_years,
        cp.last_service_date,
        
        -- Purchase metrics
        COUNT(vh.deal_number) AS total_purchases,
        SUM(vh.total_price) AS lifetime_purchase_value,
        AVG(vh.total_price) AS avg_purchase_value,
        MAX(vh.purchase_date) AS last_purchase_date,
        MIN(vh.purchase_date) AS first_purchase_date,
        
        -- Service metrics
        cp.lifetime_service_value,
        
        -- Vehicle preferences (for targeted campaigns)
        ARRAY_AGG(DISTINCT vh.make ORDER BY vh.purchase_date DESC) AS preferred_makes,
        ARRAY_AGG(DISTINCT vh.vehicle_condition ORDER BY vh.purchase_date DESC) AS purchase_patterns,
        
        -- Financial behavior
        AVG(vh.cash_down) AS avg_down_payment,
        COUNT(CASE WHEN vh.transaction_type = 'Cash' THEN 1 END) AS cash_purchases,
        COUNT(CASE WHEN vh.transaction_type = 'Financed' THEN 1 END) AS financed_purchases,
        COUNT(CASE WHEN vh.transaction_type = 'Lease' THEN 1 END) AS lease_purchases,
        
        -- Trade behavior (valuable for trade-up campaigns)
        COUNT(CASE WHEN vh.trade_vin IS NOT NULL THEN 1 END) AS trades_made,
        AVG(vh.trade_equity) AS avg_trade_equity
        
    FROM marketing.customer_profiles cp
    LEFT JOIN marketing.customer_vehicle_history vh ON cp.user_id = vh.user_id
    GROUP BY 
        cp.user_id, cp.custno, cp.firstname, cp.lastname, cp.email, cp.phone,
        cp.state, cp.zip, cp.age, cp.generation, cp.customer_tenure_years,
        cp.last_service_date, cp.lifetime_service_value
)

SELECT 
    user_id,
    custno,
    firstname,
    lastname,
    email,
    phone,
    state,
    zip,
    age,
    generation,
    
    -- Customer value metrics
    total_purchases,
    lifetime_purchase_value,
    lifetime_service_value,
    lifetime_purchase_value + lifetime_service_value AS total_lifetime_value,
    avg_purchase_value,
    
    -- Behavioral patterns
    preferred_makes,
    purchase_patterns,
    avg_down_payment,
    cash_purchases,
    financed_purchases,
    lease_purchases,
    trades_made,
    avg_trade_equity,
    
    -- Recency metrics
    last_purchase_date,
    first_purchase_date,
    last_service_date,
    
    CASE 
        WHEN last_purchase_date IS NULL THEN 999999
        ELSE CURRENT_DATE - last_purchase_date
    END AS days_since_last_purchase,
    
    CASE 
        WHEN last_service_date IS NULL THEN 999999
        ELSE CURRENT_DATE - last_service_date
    END AS days_since_last_service,
    
    -- Customer segmentation for marketing campaigns
    CASE 
        WHEN total_lifetime_value >= 100000 THEN 'VIP'
        WHEN total_lifetime_value >= 50000 AND total_purchases >= 2 THEN 'High Value Repeat'
        WHEN total_lifetime_value >= 25000 THEN 'High Value'
        WHEN total_purchases >= 2 THEN 'Repeat Customer'
        WHEN total_purchases = 1 AND (CURRENT_DATE - last_purchase_date) <= 365 THEN 'Recent First-Time'
        WHEN total_purchases = 1 THEN 'Historical First-Time'
        WHEN lifetime_service_value > 0 THEN 'Service Only'
        ELSE 'Prospect'
    END AS customer_segment,
    
    -- Marketing campaign targeting flags
    CASE 
        WHEN last_purchase_date >= CURRENT_DATE - INTERVAL '30 days' THEN true 
        ELSE false 
    END AS recent_buyer,
    
    CASE 
        WHEN total_purchases >= 2 
             AND (CURRENT_DATE - last_purchase_date) BETWEEN 1095 AND 1460  -- 3-4 years
        THEN true 
        ELSE false 
    END AS trade_up_candidate,
    
    CASE 
        WHEN last_service_date IS NOT NULL 
             AND (CURRENT_DATE - last_service_date) BETWEEN 90 AND 180
        THEN true 
        ELSE false 
    END AS service_due_candidate,
    
    CASE 
        WHEN lifetime_service_value = 0 
             AND last_purchase_date IS NOT NULL
             AND (CURRENT_DATE - last_purchase_date) >= 90
        THEN true 
        ELSE false 
    END AS service_acquisition_target,
    
    customer_tenure_years

FROM customer_metrics
WHERE email IS NOT NULL OR phone IS NOT NULL;  -- Must have contact method

-- ============================================================================
-- Location-Specific Customer Profiles (For Dealership-Level Marketing)
-- ============================================================================

CREATE OR REPLACE VIEW marketing.dealership_customer_profiles AS
/*
Purpose: Create clean customer profiles segmented by dealership location
Business Need: Each dealership location needs to market to their own customers
CDP Integration: These profiles sync to Segment for location-specific campaigns
*/

WITH location_customers AS (
    -- Identify customers by their primary dealership relationship
    SELECT 
        custno,
        branch AS primary_dealership,
        COUNT(*) AS transaction_count,
        MAX(contractdate) AS last_transaction_date,
        ROW_NUMBER() OVER (
            PARTITION BY custno 
            ORDER BY COUNT(*) DESC, MAX(contractdate) DESC
        ) AS dealership_rank
    FROM source_data.vehicle_sales
    WHERE custno IS NOT NULL AND branch IS NOT NULL
    GROUP BY custno, branch
)

SELECT 
    -- Standardized user ID for CDP sync (includes dealership code)
    CASE 
        WHEN lc.primary_dealership = 'FORD-MAIN' THEN CONCAT('FORD-MAIN-', cp.custno)
        WHEN lc.primary_dealership = 'TOYOTA-NORTH' THEN CONCAT('TOY-NORTH-', cp.custno)
        WHEN lc.primary_dealership = 'CHEV-SOUTH' THEN CONCAT('CHEV-SOUTH-', cp.custno)
        ELSE CONCAT('AUTO-', lc.primary_dealership, '-', cp.custno)
    END AS user_id,
    
    -- Customer core information
    cp.custno,
    cp.firstname AS first_name,
    cp.lastname AS last_name,
    cp.email,
    cp.phone,
    cp.address,
    cp.address2,
    cp.city,
    cp.state,
    cp.zip,
    
    -- Dealership relationship
    CASE 
        WHEN lc.primary_dealership = 'FORD-MAIN' THEN 'Downtown Ford'
        WHEN lc.primary_dealership = 'TOYOTA-NORTH' THEN 'North Toyota'
        WHEN lc.primary_dealership = 'CHEV-SOUTH' THEN 'South Chevrolet'
        ELSE lc.primary_dealership
    END AS dealership,
    
    lc.primary_dealership AS dealership_code,
    lc.transaction_count,
    lc.last_transaction_date,
    
    -- Demographics
    cp.gender,
    cp.age,
    cp.generation,
    cp.preferred_language,
    
    -- Marketing preferences
    cp.email_opt_out,
    cp.phone_opt_out,
    cp.mail_opt_out,
    cp.marketing_opt_out,
    cp.opt_out_date,
    
    -- Customer value
    cp.ytd_purchases,
    cp.lifetime_service_value,
    cp.last_service_date,
    cp.service_recency,
    
    -- Account information
    cp.customer_since,
    cp.customer_tenure_years,
    cp.last_updated

FROM marketing.customer_profiles cp
INNER JOIN location_customers lc ON cp.custno = lc.custno
WHERE 
    lc.dealership_rank = 1                      -- Primary dealership relationship
    AND cp.email IS NOT NULL                    -- Must have email for marketing
    AND cp.email_opt_out = false               -- Must be opted in for email
    AND cp.marketing_opt_out = false           -- Must be opted in for marketing
    AND lc.primary_dealership IS NOT NULL;     -- Must have dealership assignment

-- ============================================================================
-- Vehicle Sales Matched to Clean Customer Profiles
-- ============================================================================

CREATE OR REPLACE VIEW marketing.validated_vehicle_sales AS
/*
Purpose: Only include vehicle sales that match to clean, marketable customer profiles
Business Logic: Ensures marketing campaigns only target valid, contactable customers
Data Quality: Filters out bad data that can't be used for marketing activation
*/

WITH ranked_sales AS (
    SELECT 
        vs.*,
        ROW_NUMBER() OVER (
            PARTITION BY vs.dealno 
            ORDER BY vs.rowlastupdatedutc DESC
        ) AS sale_rank
    FROM source_data.vehicle_sales vs
    INNER JOIN marketing.customer_profiles cp ON vs.custno = cp.custno
    WHERE 
        vs.custno IS NOT NULL
        AND vs.vin IS NOT NULL
        AND vs.contractdate IS NOT NULL
        AND cp.email IS NOT NULL           -- Customer must have valid email
        AND cp.email_opt_out = false      -- Customer must be opted in
)

SELECT 
    -- Deal identification
    rs.dealno AS deal_number,
    CONCAT('AUTO-', rs.custno) AS user_id,
    rs.custno,
    
    -- Vehicle details
    rs.vin,
    rs.stockno AS stock_number,
    rs.makename AS make,
    rs.modelname AS model,
    rs.year,
    rs.bodystyle AS body_style,
    rs.color,
    rs.modeltype AS vehicle_type,
    rs.vehiclemileage AS mileage,
    
    -- Transaction details
    rs.saletype AS sale_type,
    rs.dealtype AS deal_type,
    rs.branch AS dealership_code,
    rs.contractdate AS sale_date,
    rs.salesdate AS delivery_date,
    
    -- Financial information
    rs.cashprice AS vehicle_price,
    rs.outthedoorprice AS total_price,
    rs.totalgross AS gross_profit,
    rs.customercashdown AS customer_down,
    rs.financesource AS lender,
    rs.financeamt AS amount_financed,
    rs.apr AS interest_rate,
    rs.term AS finance_term,
    rs.paymentamt AS monthly_payment,
    
    -- Trade-in information
    rs.trade1vin AS trade_vin,
    rs.trade1acv AS trade_value,
    rs.nettrade1 AS trade_equity,
    CONCAT(rs.trade1year, ' ', rs.trade1makename, ' ', rs.trade1modelname) AS trade_description,
    
    -- Sales team
    rs.crmsalesmgrname AS sales_manager,
    rs.crmsp1name AS salesperson,
    
    -- System tracking
    rs.rowlastupdatedutc AS last_updated,
    
    -- Marketing flags
    'Vehicle Sale' AS data_source,
    
    -- Purchase sequence
    ROW_NUMBER() OVER (
        PARTITION BY rs.custno 
        ORDER BY rs.contractdate ASC
    ) AS customer_purchase_sequence

FROM ranked_sales rs
WHERE rs.sale_rank = 1                         -- Most recent version of each deal
ORDER BY rs.contractdate DESC;

-- ============================================================================
-- GDPR/CCPA Compliance View
-- ============================================================================

CREATE OR REPLACE VIEW marketing.data_privacy_status AS
/*
Purpose: Track customer data privacy preferences and compliance status
Regulatory Need: GDPR, CCPA, and automotive industry privacy requirements
Business Impact: Ensures marketing campaigns respect customer preferences
*/

SELECT 
    CONCAT('AUTO-', custno) AS user_id,
    custno,
    firstname,
    lastname,
    email,
    
    -- Privacy preferences
    email_opt_out,
    phone_opt_out,
    mail_opt_out,
    marketing_opt_out AS general_opt_out,
    opt_out_date,
    
    -- Data processing status
    CASE 
        WHEN marketing_opt_out = true THEN 'Opted Out - No Marketing'
        WHEN email_opt_out = true AND phone_opt_out = true THEN 'Limited Contact Only'
        WHEN email_opt_out = true THEN 'Phone/Mail Only'
        WHEN phone_opt_out = true THEN 'Email/Mail Only'
        ELSE 'Full Marketing Consent'
    END AS marketing_status,
    
    -- Compliance flags for CDP sync
    NOT COALESCE(marketing_opt_out, false) AS can_market,
    NOT COALESCE(email_opt_out, false) AS can_email,
    NOT COALESCE(phone_opt_out, false) AS can_call,
    NOT COALESCE(mail_opt_out, false) AS can_mail,
    
    last_updated AS preferences_last_updated

FROM marketing.customer_profiles
WHERE email IS NOT NULL OR phone IS NOT NULL;

-- ============================================================================
-- View Usage Notes for Marketing Team
-- ============================================================================

/*
Marketing View Usage Guide:
==========================

1. customer_profiles: 
   - Foundation for all customer marketing
   - Use for general segmentation and targeting
   - Includes data quality validation and GDPR compliance

2. customer_vehicle_history:
   - Complete purchase timeline for each customer
   - Use for trade-up campaigns, loyalty programs
   - Includes financial behavior analysis

3. customer_segments:
   - Pre-calculated customer value and behavioral segments
   - Use for campaign targeting and personalization
   - Includes marketing flags for automated triggers

4. dealership_customer_profiles:
   - Location-specific customer profiles
   - Use for dealership-level marketing campaigns
   - Formatted for Segment CDP sync with location codes

5. validated_vehicle_sales:
   - Only sales matched to clean customer profiles
   - Use for accurate campaign attribution
   - Ensures marketing targets contactable customers

6. data_privacy_status:
   - Customer privacy preferences and compliance
   - Use to respect opt-outs and regulatory requirements
   - Essential for GDPR/CCPA compliance

Integration with Segment CDP:
============================
- All views use standardized user_id format for consistent tracking
- Privacy preferences automatically filter out opted-out customers
- Customer segments enable real-time personalization
- Purchase events trigger automated campaign sequences
*/
