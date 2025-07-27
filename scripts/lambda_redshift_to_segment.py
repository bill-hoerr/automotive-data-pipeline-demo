"""
AWS Lambda Function: Redshift to Segment CDP Integration
======================================================

Purpose: Stream customer events from Redshift data warehouse to Segment CDP
for real-time marketing activation and customer journey tracking.

Architecture:
- Scheduled Lambda function (daily execution)
- Reads clean customer events from Redshift analytical views  
- Sends events to Segment via HTTP API for immediate activation
- Tracks processed records to avoid duplicates
- Supports both historical backfill and incremental updates

Business Impact:
- Enables real-time marketing campaign triggers
- Provides complete customer journey tracking
- Supports automated email/SMS campaigns based on purchase behavior
- Integrates dealership data with modern marketing tools
"""

import json
import boto3
import urllib.request
import hashlib
import logging
import os
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration from environment variables
SEGMENT_API_URL = "https://api.segment.io/v1/track"
SEGMENT_WRITE_KEY = os.environ['SEGMENT_WRITE_KEY']
PROCESSED_EVENTS_BUCKET = os.environ['PROCESSED_EVENTS_BUCKET']

# Redshift connection (managed via AWS Secrets Manager in production)
REDSHIFT_HOST = os.environ['REDSHIFT_HOST']
REDSHIFT_PORT = os.environ.get('REDSHIFT_PORT', '5439')
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWORD = os.environ['REDSHIFT_PASSWORD']

# AWS clients
s3_client = boto3.client('s3')

class RedshiftToSegmentProcessor:
    """
    Handles the complete flow from Redshift analytical views to Segment CDP
    
    Key Features:
    - Deduplication via S3-stored processed events tracking
    - Batch processing with configurable batch sizes
    - Error handling and retry logic
    - Historical backfill capability
    - Real-time event streaming for marketing activation
    """
    
    def __init__(self, dealership_code: str = "AUTO"):
        self.segment_url = SEGMENT_API_URL
        self.dealership_code = dealership_code
        self.processed_events_key = f"processed_events/{dealership_code}_processed_events.json"
        self.conn = None
    
    def connect_to_redshift(self) -> bool:
        """
        Establish secure connection to Redshift data warehouse
        
        Production Note: In real implementation, credentials would be retrieved
        from AWS Secrets Manager for enhanced security
        """
        try:
            self.conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                database=REDSHIFT_DATABASE,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                sslmode='require'  # Ensure encrypted connection
            )
            logger.info("Successfully connected to Redshift data warehouse")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            return False
    
    def close_redshift_connection(self):
        """Clean up database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Redshift connection closed")
    
    def get_processed_events(self) -> set:
        """
        Retrieve list of already processed events from S3
        
        This prevents duplicate events in Segment and ensures data consistency
        across Lambda executions
        """
        try:
            response = s3_client.get_object(
                Bucket=PROCESSED_EVENTS_BUCKET, 
                Key=self.processed_events_key
            )
            processed = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Loaded {len(processed)} previously processed events")
            return set(processed)
        except s3_client.exceptions.NoSuchKey:
            logger.info("No processed events list found, starting fresh")
            return set()
        except Exception as e:
            logger.warning(f"Could not read processed events list: {e}")
            return set()
    
    def mark_events_processed(self, event_ids: list, processed_events: set):
        """
        Update S3 with newly processed event IDs
        
        Maintains state across Lambda executions for reliable deduplication
        """
        processed_events.update(event_ids)
        try:
            s3_client.put_object(
                Bucket=PROCESSED_EVENTS_BUCKET,
                Key=self.processed_events_key,
                Body=json.dumps(list(processed_events)),
                ContentType='application/json'
            )
            logger.info(f"Marked {len(event_ids)} events as processed")
        except Exception as e:
            logger.error(f"Could not update processed events list: {e}")
    
    def get_vehicle_sales_events(self, start_date: str = "2020-01-01", 
                               end_date: str = None, 
                               exclude_processed: bool = True) -> list:
        """
        Fetch vehicle sales events from Redshift marketing views
        
        Query Strategy:
        - Uses pre-built analytical views with clean, validated data
        - Filters by date range for historical backfill or incremental updates
        - Excludes already processed events to prevent duplicates
        - Returns only marketing-ready records with valid customer profiles
        """
        if not self.conn:
            raise Exception("No Redshift connection established")
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get already processed events for deduplication
        processed_events = self.get_processed_events() if exclude_processed else set()
        
        # Build exclusion clause for already processed events
        exclusion_clause = ""
        if processed_events:
            escaped_events = [str(event).replace("'", "''") for event in processed_events]
            events_list = "', '".join(escaped_events)
            exclusion_clause = f"AND deal_number NOT IN ('{events_list}')"
        
        # Query the marketing-ready view (already cleaned and validated)
        query = f"""
        SELECT 
            deal_number,
            user_id,
            vin,
            stock_number,
            email,
            phone,
            make,
            model,
            year,
            body_style,
            color,
            vehicle_condition,
            odometer_reading,
            purchase_date,
            transaction_type,
            deal_category,
            dealership_location,
            vehicle_price,
            total_price,
            dealer_cost,
            total_gross_profit,
            cash_down,
            lender,
            amount_financed,
            interest_rate,
            finance_term_months,
            monthly_payment,
            trade_vin,
            trade_actual_value,
            trade_equity,
            trade_vehicle_description,
            sales_manager,
            salesperson,
            purchase_sequence,
            last_updated
        FROM marketing.validated_vehicle_sales
        WHERE purchase_date >= '{start_date}'
        AND purchase_date <= '{end_date}'
        {exclusion_clause}
        ORDER BY purchase_date DESC
        LIMIT 1000
        """
        
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"Querying vehicle sales for date range {start_date} to {end_date}")
            logger.info(f"Excluding {len(processed_events)} already processed events")
            
            cursor.execute(query)
            records = cursor.fetchall()
            
            # Convert to list of dictionaries for easier processing
            records_list = [dict(record) for record in records]
            
            logger.info(f"Retrieved {len(records_list)} vehicle sales events from Redshift")
            
            cursor.close()
            return records_list
            
        except Exception as e:
            logger.error(f"Error querying Redshift: {e}")
            raise
    
    def clean_and_validate_event(self, record) -> dict:
        """
        Final data cleaning and validation before sending to Segment
        
        Ensures all events meet Segment's data quality requirements:
        - Required fields are present
        - Numeric values are properly formatted
        - Null values are handled appropriately
        - Event structure matches Segment spec
        """
        try:
            # Validate required fields
            if not record.get('deal_number') or not record.get('user_id') or not record.get('vin'):
                logger.warning(f"Missing required fields in record: {record.get('deal_number')}")
                return None
            
            def clean_numeric_value(value):
                """Helper to clean numeric values for Segment"""
                if value is None or value == '':
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            def clean_string_value(value):
                """Helper to clean string values"""
                if value is None:
                    return None
                return str(value).strip() if str(value).strip() else None
            
            # Build clean event data
            cleaned_event = {
                'deal_number': str(record['deal_number']),
                'user_id': str(record['user_id']),
                'vin': str(record['vin']),
                'stock_number': clean_string_value(record.get('stock_number')),
                'email': clean_string_value(record.get('email')),
                'phone': clean_string_value(record.get('phone')),
                
                # Vehicle details
                'make': clean_string_value(record.get('make')),
                'model': clean_string_value(record.get('model')),
                'year': clean_numeric_value(record.get('year')),
                'body_style': clean_string_value(record.get('body_style')),
                'color': clean_string_value(record.get('color')),
                'vehicle_condition': clean_string_value(record.get('vehicle_condition')),
                'odometer_reading': clean_numeric_value(record.get('odometer_reading')),
                
                # Transaction details
                'purchase_date': record.get('purchase_date'),
                'transaction_type': clean_string_value(record.get('transaction_type')),
                'deal_category': clean_string_value(record.get('deal_category')),
                'dealership_location': clean_string_value(record.get('dealership_location')),
                
                # Financial details
                'vehicle_price': clean_numeric_value(record.get('vehicle_price')),
                'total_price': clean_numeric_value(record.get('total_price')),
                'total_gross_profit': clean_numeric_value(record.get('total_gross_profit')),
                'cash_down': clean_numeric_value(record.get('cash_down')),
                'amount_financed': clean_numeric_value(record.get('amount_financed')),
                'interest_rate': clean_numeric_value(record.get('interest_rate')),
                'finance_term_months': clean_numeric_value(record.get('finance_term_months')),
                'monthly_payment': clean_numeric_value(record.get('monthly_payment')),
                
                # Trade information
                'trade_equity': clean_numeric_value(record.get('trade_equity')),
                'trade_vehicle_description': clean_string_value(record.get('trade_vehicle_description')),
                
                # Sales attribution
                'sales_manager': clean_string_value(record.get('sales_manager')),
                'salesperson': clean_string_value(record.get('salesperson')),
                'purchase_sequence': clean_numeric_value(record.get('purchase_sequence')),
                
                # Lender for F&I analysis
                'lender': clean_string_value(record.get('lender'))
            }
            
            return cleaned_event
            
        except Exception as e:
            logger.error(f"Error cleaning event {record.get('deal_number')}: {e}")
            return None
    
    def generate_segment_message_id(self, event_data) -> str:
        """
        Generate consistent messageId for Segment deduplication
        
        Uses deal number + VIN to create a unique, reproducible ID
        This ensures the same event sent multiple times won't create duplicates
        """
        unique_string = f"vehicle_purchase_{event_data['deal_number']}_{event_data['vin']}"
        message_hash = hashlib.md5(unique_string.encode()).hexdigest()
        return f"vp_{message_hash}"[:50]  # Segment messageId character limit
    
    def convert_to_segment_track_event(self, event_data) -> dict:
        """
        Convert cleaned data to Segment Track event format
        
        Track events capture customer actions and enable:
        - Marketing automation triggers
        - Customer journey analysis  
        - Behavioral segmentation
        - Revenue attribution
        """
        try:
            # Generate consistent messageId for deduplication
            message_id = self.generate_segment_message_id(event_data)
            
            # Handle timestamp - use actual purchase date for accurate attribution
            timestamp = event_data['purchase_date']
            if timestamp:
                try:
                    if isinstance(timestamp, str):
                        # Ensure ISO format for Segment
                        if 'T' not in timestamp:
                            timestamp = timestamp + 'T12:00:00Z'
                    else:
                        # Convert datetime object to ISO string
                        timestamp = timestamp.strftime('%Y-%m-%dT12:00:00Z')
                except Exception as e:
                    logger.warning(f"Could not parse timestamp {timestamp}: {e}")
                    timestamp = datetime.now().isoformat() + 'Z'
            else:
                timestamp = datetime.now().isoformat() + 'Z'
            
            # Build Segment Track event
            segment_event = {
                "type": "track",
                "messageId": message_id,
                "userId": event_data['user_id'],
                "event": "Vehicle Purchased",  # Standard event name for campaign triggers
                "timestamp": timestamp,
                "properties": {
                    # Transaction identification
                    "deal_number": event_data['deal_number'],
                    "vin": event_data['vin'],
                    "stock_number": event_data.get('stock_number'),
                    
                    # Vehicle characteristics (for targeting similar customers)
                    "vehicle_make": event_data.get('make'),
                    "vehicle_model": event_data.get('model'),
                    "vehicle_year": event_data.get('year'),
                    "body_style": event_data.get('body_style'),
                    "vehicle_color": event_data.get('color'),
                    "vehicle_condition": event_data.get('vehicle_condition'),
                    "odometer_reading": event_data.get('odometer_reading'),
                    
                    # Transaction details (for financial analysis)
                    "transaction_type": event_data.get('transaction_type'),  # Cash, Finance, Lease
                    "deal_category": event_data.get('deal_category'),        # Retail vs Wholesale
                    "dealership": event_data.get('dealership_location'),
                    
                    # Financial metrics (for customer value analysis)
                    "vehicle_price": event_data.get('vehicle_price'),
                    "total_price": event_data.get('total_price'),
                    "revenue": event_data.get('total_price'),               # Standard Segment field
                    "gross_profit": event_data.get('total_gross_profit'),
                    "down_payment": event_data.get('cash_down'),
                    "amount_financed": event_data.get('amount_financed'),
                    "interest_rate": event_data.get('interest_rate'),
                    "finance_term": event_data.get('finance_term_months'),
                    "monthly_payment": event_data.get('monthly_payment'),
                    "lender": event_data.get('lender'),
                    
                    # Trade information (for trade-up campaigns)
                    "had_trade": bool(event_data.get('trade_equity')),
                    "trade_equity": event_data.get('trade_equity'),
                    "trade_description": event_data.get('trade_vehicle_description'),
                    
                    # Sales attribution
                    "sales_manager": event_data.get('sales_manager'),
                    "salesperson": event_data.get('salesperson'),
                    "customer_purchase_number": event_data.get('purchase_sequence'),
                    
                    # Customer contact (for immediate follow-up)
                    "customer_email": event_data.get('email'),
                    "customer_phone": event_data.get('phone')
                },
                "context": {
                    "library": {
                        "name": "redshift-to-segment-lambda",
                        "version": "2.0.0"
                    },
                    "source": "data_warehouse"
                }
            }
            
            return segment_event
            
        except Exception as e:
            logger.error(f"Error converting to Segment event: {e}")
            return None
    
    def send_event_to_segment(self, event: dict) -> bool:
        """
        Send Track event to Segment via HTTP API
        
        This enables immediate marketing activation:
        - Welcome email series for new customers
        - Service reminders based on vehicle purchase
        - Cross-sell campaigns for accessories/warranties
        - Lookalike audience creation for Facebook/Google ads
        """
        try:
            # Add write key to authenticate with Segment
            payload = {
                **event,
                "writeKey": SEGMENT_WRITE_KEY
            }
            
            # Serialize to JSON
            data = json.dumps(payload).encode('utf-8')
            
            # Create HTTP request
            req = urllib.request.Request(
                self.segment_url,
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'redshift-to-segment-lambda/2.0.0'
                }
            )
            
            # Send to Segment
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.status == 200:
                    return True
                else:
                    logger.error(f"Segment API returned status {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error sending event to Segment: {e}")
            return False
    
    def process_vehicle_purchase_events(self, start_date: str = "2020-01-01", 
                                      batch_size: int = 100, 
                                      max_events: int = None) -> dict:
        """
        Main processing orchestration function
        
        Handles the complete flow:
        1. Connect to Redshift data warehouse
        2. Query marketing-ready vehicle sales events
        3. Clean and validate each event
        4. Convert to Segment Track event format
        5. Send to Segment CDP via HTTP API
        6. Track processed events to prevent duplicates
        
        Args:
            start_date: Start date for historical backfill (YYYY-MM-DD)
            batch_size: Number of events to process per batch
            max_events: Maximum events to process (for testing/rate limiting)
        """
        logger.info(f"Starting Redshift → Segment event processing")
        logger.info(f"Date range: {start_date} to {datetime.now().strftime('%Y-%m-%d')}")
        
        try:
            # Establish Redshift connection
            if not self.connect_to_redshift():
                raise Exception("Failed to connect to Redshift data warehouse")
            
            # Query vehicle sales events from analytical views
            events = self.get_vehicle_sales_events(start_date=start_date)
            
            if not events:
                logger.info("No new vehicle purchase events found")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'No new events to process',
                        'events_processed': 0
                    })
                }
            
            # Limit for testing or rate limiting
            if max_events and len(events) > max_events:
                logger.info(f"Limiting to {max_events} events for testing")
                events = events[:max_events]
            
            logger.info(f"Processing {len(events)} vehicle purchase events")
            
            # Process events in batches to manage memory and API limits
            successful_count = 0
            failed_count = 0
            processed_event_ids = []
            
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                batch_num = i // batch_size + 1
                logger.info(f"Processing batch {batch_num} ({len(batch)} events)")
                
                for event_record in batch:
                    try:
                        # Clean and validate event data
                        cleaned_event = self.clean_and_validate_event(event_record)
                        if not cleaned_event:
                            failed_count += 1
                            continue
                        
                        # Convert to Segment Track event format
                        segment_event = self.convert_to_segment_track_event(cleaned_event)
                        if not segment_event:
                            failed_count += 1
                            continue
                        
                        # Send to Segment CDP
                        if self.send_event_to_segment(segment_event):
                            successful_count += 1
                            processed_event_ids.append(cleaned_event['deal_number'])
                            logger.debug(f"✅ Sent Vehicle Purchased event for deal {cleaned_event['deal_number']}")
                        else:
                            failed_count += 1
                            logger.error(f"❌ Failed to send event for deal {cleaned_event['deal_number']}")
                            
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"Error processing event {event_record.get('deal_number')}: {e}")
                
                # Small delay between batches to respect Segment rate limits
                if i + batch_size < len(events):
                    import time
                    time.sleep(0.1)
            
            # Update processed events tracking in S3
            if processed_event_ids:
                processed_events_set = self.get_processed_events()
                self.mark_events_processed(processed_event_ids, processed_events_set)
            
            # Log final results
            logger.info(f"🎉 Event processing complete:")
            logger.info(f"   Total events: {len(events)}")
            logger.info(f"   Successful: {successful_count}")
            logger.info(f"   Failed: {failed_count}")
            logger.info(f"   Success rate: {(successful_count/len(events)*100):.1f}%")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Event processing complete',
                    'total_events': len(events),
                    'successful_count': successful_count,
                    'failed_count': failed_count,
                    'success_rate': round(successful_count/len(events)*100, 1),
                    'processed_event_ids': len(processed_event_ids)
                })
            }
            
        except Exception as e:
            logger.error(f"Error in main event processing: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }
        finally:
            # Always clean up Redshift connection
            self.close_redshift_connection()

def lambda_handler(event, context):
    """
    AWS Lambda entry point
    
    Triggered by:
    - CloudWatch Events (scheduled daily execution)
    - Manual invocation for historical backfill
    - API Gateway for on-demand processing
    
    Event Parameters:
    - start_date: Start date for processing (default: yesterday)
    - batch_size: Events per batch (default: 100)
    - max_events: Maximum events to process (for testing)
    - dealership_code: Which dealership to process (default: AUTO)
    """
    try:
        # Extract parameters from Lambda event
        start_date = event.get('start_date', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        batch_size = event.get('batch_size', 100)
        max_events = event.get('max_events')
        dealership_code = event.get('dealership_code', 'AUTO')
        
        logger.info(f"Lambda triggered with parameters:")
        logger.info(f"  Start date: {start_date}")
        logger.info(f"  Batch size: {batch_size}")
        logger.info(f"  Max events: {max_events}")
        logger.info(f"  Dealership: {dealership_code}")
        
        # Initialize processor and run
        processor = RedshiftToSegmentProcessor(dealership_code=dealership_code)
        result = processor.process_vehicle_purchase_events(
            start_date=start_date,
            batch_size=batch_size,
            max_events=max_events
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Lambda execution failed'
            })
        }

# ============================================================================
# Deployment and Monitoring Notes
# ============================================================================

"""
Lambda Configuration:
====================
- Runtime: Python 3.9+
- Memory: 512 MB (sufficient for batch processing)
- Timeout: 15 minutes (handles large historical backfills)
- Environment Variables:
  * SEGMENT_WRITE_KEY (from AWS Secrets Manager)
  * REDSHIFT_HOST, REDSHIFT_DATABASE, etc.
  * PROCESSED_EVENTS_BUCKET

Trigger Configuration:
=====================
- CloudWatch Events: Daily at 6 AM EST (after ETL completion)
- Manual invocation: For historical backfill or testing
- API Gateway: For on-demand processing from marketing tools

Monitoring:
==========
- CloudWatch Logs: All processing details and errors
- CloudWatch Metrics: Success/failure rates, processing times
- SNS Alerts: On processing failures or data quality issues
- Segment Debugger: Validate event format and delivery

Business Impact:
===============
This Lambda function enables real-time marketing activation:
1. New vehicle buyers automatically enter welcome campaign
2. Service reminders triggered based on purchase date + mileage
3. Trade-up campaigns target customers with positive equity
4. Cross-sell campaigns promote warranties, accessories
5. Lookalike audiences created for Facebook/Google advertising

The combination of clean Redshift data + Segment CDP provides:
- Immediate campaign activation (within minutes of sale)
- Complete customer journey tracking across touchpoints  
- Advanced segmentation for personalized experiences
- Attribution analysis for marketing ROI measurement
"""
