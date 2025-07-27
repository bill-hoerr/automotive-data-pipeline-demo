import paramiko
import os
import boto3
import gnupg
from datetime import datetime
import logging
from pathlib import Path
import re

class DealershipDataProcessor:
    """
    Automated pipeline for processing dealership system exports
    
    Handles the daily challenge of extracting data from legacy automotive systems:
    - Downloads encrypted CSV files from vendor SFTP servers
    - Decrypts files using GPG (automotive vendors require encryption)
    - Uploads to S3 with organized partitioning for downstream ETL
    """
    
    def __init__(self, sftp_config, gpg_config, s3_config):
        """
        Initialize the processor with configuration settings
        """
        self.sftp_config = sftp_config
        self.gpg_config = gpg_config
        self.s3_config = s3_config
        
        # Setup comprehensive logging for daily automation
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('dealership_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize AWS S3 client
        self.s3_client = boto3.client('s3', region_name=s3_config['region'])
        
        # Create local processing directories
        self.base_dir = Path('/opt/dealership_automation')
        self.local_base_path = self.base_dir / 'daily_exports'
        self.encrypted_path = self.local_base_path / 'encrypted'
        self.decrypted_path = self.local_base_path / 'decrypted'
        
        # Ensure clean processing environment
        for path in [self.encrypted_path, self.decrypted_path]:
            if path.exists():
                # Clean up any existing files from previous runs
                for file in path.glob('*'):
                    try:
                        file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Could not remove old file {file}: {e}")
            else:
                path.mkdir(parents=True, exist_ok=True)

    def connect_sftp(self):
        """
        Establish SFTP connection to dealership vendor system
        
        Most automotive vendors use SFTP with password auth (not key-based)
        due to legacy infrastructure limitations
        """
        try:
            transport = paramiko.Transport((
                self.sftp_config['hostname'],
                self.sftp_config['port']
            ))
            transport.connect(
                username=self.sftp_config['username'],
                password=self.sftp_config['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            self.logger.info(f"Successfully connected to {self.sftp_config['hostname']}")
            return sftp, transport
        except Exception as e:
            self.logger.error(f"Failed to connect to SFTP: {str(e)}")
            raise

    def download_files(self, table_list):
        """
        Download latest files for specified tables from vendor SFTP server
        
        Challenge: Automotive vendors typically export files with timestamps,
        need to identify and download only the most recent file per table
        """
        downloaded_files = []
        sftp, transport = self.connect_sftp()
        
        try:
            # Vendor-specific directory structure (each vendor is different)
            vendor_directory = self.sftp_config['base_directory']
            
            for table in table_list:
                try:
                    table_path = f"{vendor_directory}/{table}"
                    self.logger.info(f"Processing table: {table}")
                    
                    # Get all encrypted files with modification times
                    files_with_attrs = []
                    for filename in sftp.listdir(table_path):
                        if filename.endswith('.csv.gpg'):  # Vendor encrypts all exports
                            file_stats = sftp.stat(f"{table_path}/{filename}")
                            mod_time = datetime.fromtimestamp(file_stats.st_mtime)
                            files_with_attrs.append((filename, file_stats.st_mtime))
                            self.logger.info(f"  Found: {filename} - {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if not files_with_attrs:
                        self.logger.warning(f"No encrypted files found for {table}")
                        continue
                        
                    # Sort by modification time and get the latest
                    files_with_attrs.sort(key=lambda x: x[1], reverse=True)
                    latest_file = files_with_attrs[0][0]
                    
                    # Download the most recent export
                    remote_path = f"{table_path}/{latest_file}"
                    local_path = self.encrypted_path / latest_file
                    
                    self.logger.info(f"Downloading latest file: {latest_file}")
                    sftp.get(remote_path, str(local_path))
                    downloaded_files.append(local_path)
                    
                except FileNotFoundError:
                    self.logger.error(f"Directory not found: {table}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error downloading {table}: {str(e)}")
                    continue
                    
        finally:
            sftp.close()
            transport.close()
            
        self.logger.info(f"Downloaded {len(downloaded_files)} files successfully")
        return downloaded_files

    def decrypt_files(self, encrypted_file_paths):
        """
        Decrypt GPG-encrypted CSV files from automotive vendor
        
        Security requirement: All automotive data exports are GPG encrypted
        due to PII and financial information in customer records
        """
        decrypted_files = []
        gpg = gnupg.GPG(gnupghome=self.gpg_config['gpg_home'])
        
        for file_path in encrypted_file_paths:
            try:
                self.logger.info(f"Decrypting: {file_path.name}")
                decrypted_path = self.decrypted_path / file_path.name.replace('.gpg', '')
                
                with open(str(file_path), 'rb') as encrypted_file:
                    decryption_result = gpg.decrypt_file(
                        encrypted_file,
                        output=str(decrypted_path),
                        always_trust=True,
                        passphrase=self.gpg_config['passphrase'],
                        extra_args=['--recipient', self.gpg_config['key_id']]
                    )
                
                if decryption_result.ok:
                    self.logger.info(f"Successfully decrypted: {file_path.name}")
                    decrypted_files.append(decrypted_path)
                else:
                    self.logger.error(f"Decryption failed for {file_path.name}: {decryption_result.status}")
                    
            except Exception as e:
                self.logger.error(f"Error decrypting {file_path.name}: {str(e)}")
                
        return decrypted_files

    def upload_to_s3(self, decrypted_file_paths):
        """
        Upload processed files to S3 with date partitioning
        
        Partitioning strategy enables efficient Glue ETL processing
        and historical data analysis in Redshift
        """
        successful_uploads = []
        
        for file_path in decrypted_file_paths:
            try:
                # Extract table name and date from vendor filename pattern
                # Typical format: VENDOR_TableName_Export_YYYY-MM-DD.csv
                filename_pattern = r'(\w+)_([A-Za-z]+)_\w+_(\d{4}-\d{2}-\d{2})'
                match = re.match(filename_pattern, file_path.name)
                
                if not match:
                    self.logger.error(f"Filename doesn't match expected pattern: {file_path.name}")
                    continue
                    
                vendor_code, table_name, export_date = match.groups()
                year, month, day = export_date.split('-')
                
                # S3 key structure optimized for Glue crawlers and Athena queries
                s3_key = f"raw-data/{table_name}/year={year}/month={month}/day={day}/{file_path.name}"
                
                self.logger.info(f"Uploading to S3: {s3_key}")
                self.s3_client.upload_file(
                    str(file_path),
                    self.s3_config['bucket_name'],
                    s3_key,
                    ExtraArgs={
                        'ServerSideEncryption': 'AES256',  # Encrypt at rest
                        'Metadata': {
                            'source_table': table_name,
                            'export_date': export_date,
                            'processed_date': datetime.now().strftime('%Y-%m-%d'),
                            'vendor': vendor_code
                        }
                    }
                )
                
                successful_uploads.append(s3_key)
                self.logger.info(f"Successfully uploaded: {s3_key}")
                
            except Exception as e:
                self.logger.error(f"S3 upload failed for {file_path.name}: {str(e)}")
                
        return successful_uploads

    def cleanup_local_files(self, file_paths):
        """
        Clean up local files after successful processing
        Prevents disk space issues in automated daily runs
        """
        for file_path in file_paths:
            try:
                file_path.unlink()
                self.logger.info(f"Cleaned up local file: {file_path.name}")
            except Exception as e:
                self.logger.error(f"Cleanup failed for {file_path}: {str(e)}")

    def trigger_glue_etl(self, uploaded_files):
        """
        Trigger downstream ETL processing after successful S3 upload
        
        This starts the Glue Visual ETL jobs that transform raw vendor data
        into clean, standardized format for Redshift data warehouse
        """
        if not uploaded_files:
            self.logger.warning("No files uploaded, skipping ETL trigger")
            return
            
        try:
            glue_client = boto3.client('glue', region_name=self.s3_config['region'])
            
            # Trigger the main ETL job that processes all tables
            job_run_response = glue_client.start_job_run(
                JobName='dealership-daily-etl',
                Arguments={
                    '--S3_INPUT_PATH': f"s3://{self.s3_config['bucket_name']}/raw-data/",
                    '--PROCESSING_DATE': datetime.now().strftime('%Y-%m-%d')
                }
            )
            
            job_run_id = job_run_response['JobRunId']
            self.logger.info(f"Triggered Glue ETL job: {job_run_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to trigger ETL job: {str(e)}")

    def process_daily_exports(self):
        """
        Main orchestration function for daily dealership data processing
        
        This runs every morning via cron to process the previous day's
        dealership system exports and prepare data for marketing activation
        """
        # Define all tables we need from the dealership system
        required_tables = [
            'Customer',           # Customer master data
            'Vehicle',           # Vehicle inventory
            'VehicleSales',      # Sales transactions
            'ServiceAppointments', # Service bookings
            'ServiceHistory',    # Completed service work
            'PartsInventory',    # Parts availability
            'PartsSales',        # Parts transactions
            'Employee',          # Staff information
            'InventoryVehicle',  # Current lot inventory
            'SpecialOrders'      # Custom vehicle orders
        ]
        
        processing_start = datetime.now()
        self.logger.info(f"Starting daily processing at {processing_start}")
        
        try:
            # Step 1: Download encrypted files from vendor SFTP
            self.logger.info("Phase 1: Downloading files from vendor SFTP")
            downloaded_files = self.download_files(required_tables)
            
            if not downloaded_files:
                self.logger.error("No files downloaded - aborting processing")
                return False
            
            # Step 2: Decrypt all downloaded files
            self.logger.info("Phase 2: Decrypting vendor files")
            decrypted_files = self.decrypt_files(downloaded_files)
            
            if not decrypted_files:
                self.logger.error("No files decrypted successfully - aborting")
                return False
            
            # Step 3: Upload to S3 data lake with proper partitioning
            self.logger.info("Phase 3: Uploading to S3 data lake")
            uploaded_files = self.upload_to_s3(decrypted_files)
            
            # Step 4: Trigger downstream ETL processing
            self.logger.info("Phase 4: Triggering Glue ETL jobs")
            self.trigger_glue_etl(uploaded_files)
            
            # Step 5: Clean up local files
            self.logger.info("Phase 5: Cleaning up local files")
            all_files = downloaded_files + decrypted_files
            self.cleanup_local_files(all_files)
            
            processing_duration = datetime.now() - processing_start
            self.logger.info(f"Daily processing completed successfully in {processing_duration}")
            return True
            
        except Exception as e:
            self.logger.error(f"Daily processing failed: {str(e)}")
            return False

def main():
    """
    Daily automation entry point
    
    Configuration would typically be loaded from environment variables
    or AWS Systems Manager Parameter Store in production
    """
    
    # SFTP configuration for automotive vendor system
    sftp_config = {
        'hostname': 'vendor-sftp.dealership-system.com',
        'username': 'DEALERSHIP_USER',
        'password': 'PASSWORD_FROM_ENV',  # Load from environment in production
        'port': 22,
        'base_directory': 'daily_exports'
    }
    
    # GPG configuration for decryption
    gpg_config = {
        'gpg_home': '/home/ubuntu/.gnupg',
        'key_id': 'GPG_KEY_ID_FROM_ENV',     # Load from environment
        'passphrase': 'GPG_PASSPHRASE_FROM_ENV'  # Load from environment
    }
    
    # S3 configuration for data lake
    s3_config = {
        'bucket_name': 'automotive-data-lake',
        'region': 'us-east-1'
    }
    
    # Initialize and run the daily processing
    processor = DealershipDataProcessor(sftp_config, gpg_config, s3_config)
    success = processor.process_daily_exports()
    
    if success:
        print("Daily processing completed successfully")
        exit(0)
    else:
        print("Daily processing failed - check logs")
        exit(1)

if __name__ == '__main__':
    main()
