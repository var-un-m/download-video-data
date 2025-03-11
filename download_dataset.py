import boto3
import csv
import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Download and process data from DynamoDB to CSV.')
    
    parser.add_argument('--table', type=str, default='crops_v5',
                        help='DynamoDB table name (default: crops_v5)')
    
    parser.add_argument('--output-csv', type=str, required=True,
                        help='Output CSV file path')
    
    parser.add_argument('--download-dir', type=str, required=True,
                        help='Base directory for downloaded files')
    
    parser.add_argument('--region', type=str, default='us-west-2',
                        help='AWS region (default: us-west-2)')
    
    parser.add_argument('--workers', type=int, default=3,
                        help='Number of download workers (default: 3)')
    
    parser.add_argument('--quality-threshold', type=float, default=0.0,
                        help='Minimum quality score threshold for videos to process (default: 0.0)')
    
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Logging level (default: INFO)')
    
    return parser.parse_args()

# Configure logging
def setup_logging(log_level):
    """Set up logging with the specified log level."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def parse_s3_path(s3_path):
    """Parse an S3 path into bucket name and key."""
    if not s3_path.startswith('s3://'):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    
    path_without_prefix = s3_path[5:]
    parts = path_without_prefix.split('/', 1)
    
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 path format: {s3_path}")
    
    bucket = parts[0]
    key = parts[1]
    
    return bucket, key

def download_from_s3(s3_path, local_path, s3_client, logger):
    """Download a file from S3 to local storage and return the absolute path."""
    try:
        bucket, key = parse_s3_path(s3_path)
        logger.info(f"Downloading {s3_path} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        # Return the absolute path
        return os.path.abspath(local_path)
    except ClientError as e:
        logger.error(f"Error downloading {s3_path}: {e}")
        return None

def download_files(entry, download_base_dir, s3_client, workers, logger):
    """Download video, audio, and landmarks files for an entry."""
    video_id = entry['video_id']
    quality_score = float(entry.get('quality_score', 0.0))
    
    # Get paths
    video_path = entry['video_path']
    audio_path = entry['audio_path']
    landmarks_path = entry['landmarks_raw_path']
    
    # Create local paths (relative paths)
    rel_video_path = os.path.join(download_base_dir, 'video', os.path.basename(video_path))
    rel_audio_path = os.path.join(download_base_dir, 'audio', os.path.basename(audio_path))
    rel_landmark_path = os.path.join(download_base_dir, 'landmarks', os.path.basename(landmarks_path))
    
    # Convert to absolute paths
    local_video_path = os.path.abspath(rel_video_path)
    local_audio_path = os.path.abspath(rel_audio_path)
    local_landmark_path = os.path.abspath(rel_landmark_path)
    
    # Download files
    results = {}
    results['video_id'] = video_id
    results['quality_score'] = quality_score
    
    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=workers) as executor:
        video_future = executor.submit(download_from_s3, video_path, local_video_path, s3_client, logger)
        audio_future = executor.submit(download_from_s3, audio_path, local_audio_path, s3_client, logger)
        landmark_future = executor.submit(download_from_s3, landmarks_path, local_landmark_path, s3_client, logger)
        
        results['local_video_path'] = video_future.result()
        results['local_audio_path'] = audio_future.result()
        results['local_landmark_path'] = landmark_future.result()
    
    return results

def process_dynamo_entries(table_name, download_base_dir, region, workers, quality_threshold, logger):
    """Process entries from DynamoDB table."""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    
    table = dynamodb.Table(table_name)
    results = []
    
    # Get all items from the table
    # Note: In a production environment with a large table, you might want to use pagination
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        # Process additional pages if DynamoDB returns them
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
    except ClientError as e:
        logger.error(f"Error scanning DynamoDB table: {e}")
        return []
    
    logger.info(f"Found {len(items)} entries in DynamoDB table")
    
    # Filter entries according to requirements
    filtered_entries = []
    for item in items:
        landmarks_path = item.get('landmarks_raw_path', '')
        quality_score = float(item.get('quality_score', 0.0))
        
        # Keep only entries where:
        # 1. landmarks_raw_path contains "MP_v1"
        # 2. landmarks_raw_path does not contain "CROP_NECK_edge_filter_w_RAWLM"
        # 3. quality_score is above the threshold
        if ('MP' in landmarks_path and
            quality_score >= quality_threshold):
            filtered_entries.append(item)
    
    logger.info(f"Filtered to {len(filtered_entries)} entries with 'MP' in landmarks path and quality score >= {quality_threshold}")
    
    # Download files for filtered entries
    for entry in filtered_entries:
        result = download_files(entry, download_base_dir, s3, workers, logger)
        if all(result.values()):  # Only add if all downloads succeeded
            results.append(result)
    
    return results

def write_to_csv(results, output_file, logger):
    """Write results to CSV file."""
    if not results:
        logger.warning("No results to write to CSV")
        return
    
    fieldnames = ['video_id', 'local_video_path', 'local_audio_path', 'local_landmark_path', 'quality_score']
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    logger.info(f"Wrote {len(results)} entries to {output_file}")

def main():
    """Main function to process DynamoDB entries and create CSV."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    logger = setup_logging(args.log_level)
    
    logger.info(f"Starting DynamoDB processing with table '{args.table}'")
    logger.info(f"Quality threshold set to {args.quality_threshold}")
    
    # Create directories for downloads
    download_base_dir = args.download_dir
    os.makedirs(os.path.join(download_base_dir, 'video'), exist_ok=True)
    os.makedirs(os.path.join(download_base_dir, 'audio'), exist_ok=True)
    os.makedirs(os.path.join(download_base_dir, 'landmarks'), exist_ok=True)
    
    # Process entries
    results = process_dynamo_entries(args.table, download_base_dir, args.region, 
                                    args.workers, args.quality_threshold, logger)
    
    # Write to CSV
    write_to_csv(results, args.output_csv, logger)
    
    logger.info("Processing complete")

if __name__ == "__main__":
    main()