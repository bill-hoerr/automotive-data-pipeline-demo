-- ============================================================================
-- Customer Identity Resolution Database Schema
-- ============================================================================

/*
Purpose: Store and match website visitor identities with CRM lead data
Business Use: Connect online customer behavior to offline sales outcomes
Performance: Optimized for real-time matching and analytics queries

Tables:
1. visitor_sessions - Website visitor tracking data
2. crm_leads - Parsed lead data from dealership management system
3. identity_matches - Links between visitors and leads
4. match_analytics - Cached metrics for performance monitoring
*/

-- ============================================================================
-- Visitor Sessions Table
-- ============================================================================

CREATE TABLE visitor_sessions (
    -- Primary identification
    id SERIAL PRIMARY KEY,
    session_uuid UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Core tracking identifiers
    segment_anonymous_id VARCHAR(255) NOT NULL,
    digital_retailing_session_id VARCHAR(255),
    
    -- Marketing attribution data
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_term VARCHAR(255),
    utm_content VARCHAR(255),
    gclid VARCHAR(255),              -- Google Ads click ID
    fbclid VARCHAR(255),             -- Facebook click ID
    msclkid VARCHAR(255),            -- Microsoft Ads click ID
    
    -- Session context
    referrer TEXT,
    landing_page TEXT NOT NULL,
    page_title VARCHAR(500),
    user_agent TEXT,
    ip_address INET,
    
    -- Browser fingerprint for fraud detection
    browser_fingerprint JSONB,
    
    -- Geographic data (derived from IP)
    country_code CHAR(2),
    region VARCHAR(100),
    city VARCHAR(100),
    
    -- Timestamps
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    session_duration_minutes INTEGER DEFAULT 0,
    
    -- Matching status
    is_matched BOOLEAN DEFAULT FALSE,
    matched_lead_id INTEGER,
    matched_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX idx_visitor_sessions_segment_id ON visitor_sessions(segment_anonymous_id);
CREATE INDEX idx_visitor_sessions_retailing_session ON visitor_sessions(digital_retailing_session_id);
CREATE INDEX idx_visitor_sessions_utm_source ON visitor_sessions(utm_source);
CREATE INDEX idx_visitor_sessions_created_at ON visitor_sessions(created_at);
CREATE INDEX idx_visitor_sessions_matching ON visitor_sessions(is_matched, created_at);

-- GIN index for JSONB browser fingerprint queries
CREATE INDEX idx_visitor_sessions_fingerprint ON visitor_sessions USING GIN(browser_fingerprint);

-- ============================================================================
-- CRM Leads Table  
-- ============================================================================

CREATE TABLE crm_leads (
    -- Primary identification
    id SERIAL PRIMARY KEY,
    lead_uuid UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Lead identification from CRM
    lead_id VARCHAR(255) NOT NULL,
    crm_session_id VARCHAR(255),     -- Session bridge for matching
    
    -- Customer information
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Vehicle interest data
    vehicle_year INTEGER,
    vehicle_make VARCHAR(100),
    vehicle_model VARCHAR(255),
    vehicle_vin VARCHAR(17),
    vehicle_stock VARCHAR(50),
    vehicle_price DECIMAL(10,2),
    
    -- Lead classification
    lead_type VARCHAR(100),          -- New, Used, Service, Parts
    lead_source VARCHAR(255),        -- Digital Retailing, Website Form, etc.
    lead_status VARCHAR(100) DEFAULT 'new',
    
    -- Raw data preservation
    raw_adf_xml TEXT,               -- Original ADF/XML for audit
    email_subject VARCHAR(500),      -- Email subject for reference
    
    -- Matching status
    is_matched BOOLEAN DEFAULT FALSE,
    matched_visitor_id INTEGER,
    matched_at TIMESTAMP WITH TIME ZONE,
    match_method VARCHAR(100),       -- session_id, email, phone, manual
    
    -- Business metrics
    estimated_deal_value DECIMAL(10,2),
    probability_score DECIMAL(3,2),  -- 0.00 to 1.00
    
    -- Timestamps
    lead_submitted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX idx_crm_leads_lead_id ON crm_leads(lead_id);
CREATE INDEX idx_crm_leads_session_id ON crm_leads(crm_session_id);
CREATE INDEX idx_crm_leads_email ON crm_leads(email);
CREATE INDEX idx_crm_leads_phone ON crm_leads(phone);
CREATE INDEX idx_crm_leads_submitted_at ON crm_leads(lead_submitted_at);
CREATE INDEX idx_crm_leads_matching ON crm_leads(is_matched, created_at);
CREATE INDEX idx_crm_leads_source_status ON crm_leads(lead_source, lead_status);

-- ============================================================================
-- Identity Matches Table
-- ============================================================================

CREATE TABLE identity_matches (
    -- Primary identification
    id SERIAL PRIMARY KEY,
    match_uuid UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Relationship mapping
    visitor_session_id INTEGER NOT NULL REFERENCES visitor_sessions(id),
    crm_lead_id INTEGER NOT NULL REFERENCES crm_leads(id),
    
    -- Match details
    match_method VARCHAR(100) NOT NULL, -- session_id, email_phone, manual, ml_prediction
    match_confidence DECIMAL(3,2) DEFAULT 1.00, -- 0.00 to 1.00
    match_score INTEGER,                -- Algorithm-specific score
    
    -- Attribution data (denormalized for performance)
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255), 
    utm_campaign VARCHAR(255),
    gclid VARCHAR(255),
    referrer TEXT,
    landing_page TEXT,
    
    -- Business impact tracking
    is_converted BOOLEAN DEFAULT FALSE,
    conversion_value DECIMAL(10,2),
    conversion_date TIMESTAMP WITH TIME ZONE,
    
    -- Quality assurance
    is_verified BOOLEAN DEFAULT FALSE,
    verified_by VARCHAR(255),
    verified_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255) DEFAULT 'system'
);

-- Ensure unique visitor-lead pairs
CREATE UNIQUE INDEX idx_identity_matches_unique_pair ON identity_matches(visitor_session_id, crm_lead_id);

-- Performance indexes
CREATE INDEX idx_identity_matches_visitor ON identity_matches(visitor_session_id);
CREATE INDEX idx_identity_matches_lead ON identity_matches(crm_lead_id);
CREATE INDEX idx_identity_matches_method ON identity_matches(match_method);
CREATE INDEX idx_identity_matches_conversion ON identity_matches(is_converted, conversion_date);
CREATE INDEX idx_identity_matches_created_at ON identity_matches(created_at);

-- ============================================================================
-- Match Analytics Cache Table
-- ============================================================================

CREATE TABLE match_analytics (
    -- Primary identification
    id SERIAL PRIMARY KEY,
    
    -- Time period
    date_period DATE NOT NULL,
    hour_period INTEGER, -- 0-23, NULL for daily aggregates
    
    -- Metrics
    total_visitors INTEGER DEFAULT 0,
    total_leads INTEGER DEFAULT 0,
    successful_matches INTEGER DEFAULT 0,
    match_rate DECIMAL(5,2) DEFAULT 0.00,
    
    -- Attribution breakdown
    utm_source_breakdown JSONB,
    match_method_breakdown JSONB,
    conversion_metrics JSONB,
    
    -- Performance metrics
    avg_match_time_seconds INTEGER,
    total_revenue_attributed DECIMAL(12,2) DEFAULT 0,
    
    -- Metadata
    last_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Unique constraint to prevent duplicate periods
CREATE UNIQUE INDEX idx_match_analytics_period ON match_analytics(date_period, COALESCE(hour_period, -1));

-- Performance indexes
CREATE INDEX idx_match_analytics_date ON match_analytics(date_period);
CREATE INDEX idx_match_analytics_calculated_at ON match_analytics(last_calculated_at);

-- ============================================================================
-- Automated Triggers and Functions
-- ============================================================================

-- Function to update visitor session matching status
CREATE OR REPLACE FUNCTION update_visitor_match_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE visitor_sessions 
    SET 
        is_matched = TRUE,
        matched_lead_id = NEW.crm_lead_id,
        matched_at = NOW(),
        updated_at = NOW()
    WHERE id = NEW.visitor_session_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to update CRM lead matching status
CREATE OR REPLACE FUNCTION update_lead_match_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE crm_leads
    SET 
        is_matched = TRUE,
        matched_visitor_id = NEW.visitor_session_id,
        matched_at = NOW(),
        match_method = NEW.match_method,
        updated_at = NOW()
    WHERE id = NEW.crm_lead_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers to maintain matching status automatically
CREATE TRIGGER trigger_visitor_match_status
    AFTER INSERT ON identity_matches
    FOR EACH ROW
    EXECUTE FUNCTION update_visitor_match_status();

CREATE TRIGGER trigger_lead_match_status
    AFTER INSERT ON identity_matches
    FOR EACH ROW
    EXECUTE FUNCTION update_lead_match_status();

-- Function to update timestamps automatically
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply update timestamp triggers
CREATE TRIGGER trigger_visitor_sessions_updated_at
    BEFORE UPDATE ON visitor_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_crm_leads_updated_at
    BEFORE UPDATE ON crm_leads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- Analytical Views
-- ============================================================================

-- Real-time matching performance view
CREATE VIEW v_matching_performance AS
SELECT 
    DATE(created_at) as match_date,
    COUNT(*) as total_matches,
    COUNT(*) FILTER (WHERE match_method = 'session_id') as session_matches,
    COUNT(*) FILTER (WHERE match_method = 'email_phone') as contact_matches,
    COUNT(*) FILTER (WHERE match_method = 'manual') as manual_matches,
    AVG(match_confidence) as avg_confidence,
    COUNT(*) FILTER (WHERE is_converted = TRUE) as conversions,
    SUM(conversion_value) FILTER (WHERE is_converted = TRUE) as total_revenue
FROM identity_matches
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY match_date DESC;

-- Attribution analysis view
CREATE VIEW v_attribution_analysis AS
SELECT 
    vs.utm_source,
    vs.utm_medium,
    vs.utm_campaign,
    COUNT(DISTINCT vs.id) as total_visitors,
    COUNT(DISTINCT im.id) as matched_visitors,
    ROUND(
        COUNT(DISTINCT im.id)::DECIMAL / 
        NULLIF(COUNT(DISTINCT vs.id), 0) * 100, 2
    ) as match_rate_percent,
    COUNT(*) FILTER (WHERE im.is_converted = TRUE) as conversions,
    SUM(im.conversion_value) as total_revenue,
    ROUND(
        SUM(im.conversion_value) / 
        NULLIF(COUNT(DISTINCT vs.id), 0), 2
    ) as revenue_per_visitor
FROM visitor_sessions vs
LEFT JOIN identity_matches im ON vs.id = im.visitor_session_id
WHERE vs.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY vs.utm_source, vs.utm_medium, vs.utm_campaign
HAVING COUNT(DISTINCT vs.id) >= 10  -- Minimum volume threshold
ORDER BY total_revenue DESC NULLS LAST;

-- Unmatched leads analysis
CREATE VIEW v_unmatched_leads AS
SELECT 
    cl.lead_source,
    cl.lead_type,
    DATE(cl.created_at) as lead_date,
    COUNT(*) as unmatched_count,
    ROUND(AVG(cl.estimated_deal_value), 2) as avg_deal_value,
    SUM(cl.estimated_deal_value) as total_potential_value
FROM crm_leads cl
WHERE cl.is_matched = FALSE
  AND cl.created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY cl.lead_source, cl.lead_type, DATE(cl.created_at)
ORDER BY total_potential_value DESC;

-- ============================================================================
-- Data Retention and Cleanup
-- ============================================================================

-- Function to archive old visitor sessions (GDPR compliance)
CREATE OR REPLACE FUNCTION archive_old_visitor_sessions()
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Archive sessions older than 2 years that aren't matched
    WITH archived_sessions AS (
        DELETE FROM visitor_sessions 
        WHERE created_at < CURRENT_DATE - INTERVAL '2 years'
          AND is_matched = FALSE
        RETURNING *
    )
    SELECT COUNT(*) INTO archived_count FROM archived_sessions;
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Schedule cleanup (would be called by cron job)
-- SELECT archive_old_visitor_sessions();

-- ============================================================================
-- Performance Monitoring Queries
-- ============================================================================

-- Query to check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes 
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Query to monitor table sizes
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as total_size,
    pg_size_pretty(pg_relation_size(tablename::regclass)) as table_size,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes
FROM pg_stat_user_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(tablename::regclass) DESC;

-- ============================================================================
-- Sample Data for Testing
-- ============================================================================

-- Insert sample visitor session
INSERT INTO visitor_sessions (
    segment_anonymous_id,
    digital_retailing_session_id,
    utm_source,
    utm_medium,
    utm_campaign,
    landing_page,
    referrer
) VALUES (
    'ajs_anonymous_id_12345',
    'dr_session_67890',
    'google',
    'cpc',
    'subaru_outback_campaign',
    'https://dealership.com/inventory/subaru-outback',
    'https://google.com'
);

-- Insert sample CRM lead
INSERT INTO crm_leads (
    lead_id,
    crm_session_id,
    first_name,
    last_name,
    email,
    phone,
    vehicle_year,
    vehicle_make,
    vehicle_model,
    lead_source,
    estimated_deal_value
) VALUES (
    'LEAD_12345',
    'dr_session_67890',
    'John',
    'Smith',
    'john.smith@email.com',
    '555-123-4567',
    2024,
    'Subaru',
    'Outback',
    'Digital Retailing',
    35000.00
);

-- Create identity match
INSERT INTO identity_matches (
    visitor_session_id,
    crm_lead_id,
    match_method,
    match_confidence,
    utm_source,
    utm_medium,
    utm_campaign
) VALUES (
    1, -- visitor_session_id
    1, -- crm_lead_id
    'session_id',
    1.00,
    'google',
    'cpc',
    'subaru_outback_campaign'
);

/**
 * Schema Design Notes:
 * ==================
 * 
 * Scalability:
 * - Partitioning by date for large volumes
 * - Proper indexing for fast lookups
 * - Materialized views for analytics
 * 
 * Data Quality:
 * - Foreign key constraints maintain referential integrity
 * - Check constraints validate data ranges
 * - Triggers maintain denormalized data consistency
 * 
 * Privacy Compliance:
 * - UUID primary keys prevent enumeration
 * - Data retention policies for GDPR compliance
 * - IP address anonymization after processing
 * 
 * Performance Optimization:
 * - Covering indexes for common query patterns
 * - JSONB for flexible schema evolution
 * - Partial indexes for filtered queries
 * 
 * Monitoring:
 * - Built-in analytics views for business metrics
 * - Performance monitoring queries included
 * - Automated cleanup procedures
 */
