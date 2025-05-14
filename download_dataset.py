import boto3
import csv
import os
import logging
import argparse
import math
import re
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

def calculate_num_frames(video_id):
    """Extract frame information from video_id and calculate num_frames."""
    try:
        # Extract the frame range from the video_id
        match = re.search(r'(\d+)_(\d+)$', video_id)
        if match:
            start_frame = int(match.group(1))
            end_frame = int(match.group(2))
            num_frames = end_frame - start_frame
            return num_frames
        return None
    except Exception:
        return None

def get_subfolder_number(file_count):
    """Determine the subfolder number based on file count."""
    return str(math.floor(file_count / 1000)).zfill(7)

def download_from_s3(s3_path, local_path, s3_client, logger):
    """Download a file from S3 to local storage and return the absolute path."""
    try:
        bucket, key = parse_s3_path(s3_path)
        logger.info(f"Downloading {s3_path} to {local_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        s3_client.download_file(bucket, key, local_path)
        return os.path.abspath(local_path)
    except ClientError as e:
        logger.error(f"Error downloading {s3_path}: {e}")
        return None

def download_files(entry, download_base_dir, subfolder, s3_client, workers, logger):
    """Download video, audio, and landmarks files for an entry if they exist."""
    video_id = entry['video_id']
    quality_score = float(entry.get('quality_score', 0.0))
    actor_id = entry.get('actor_id', '')
    
    # Video is required
    video_path = entry['video_path']
    
    # Audio and landmarks are optional
    audio_path = entry.get('audio_path', '')
    landmarks_path = entry.get('landmarks_raw_path', '')
    
    # Get num_frames from DynamoDB entry if it exists, otherwise calculate it
    num_frames = None
    if 'num_frames' in entry:
        try:
            num_frames = int(entry['num_frames'])
        except (ValueError, TypeError):
            logger.warning(f"Invalid num_frames value in DynamoDB for {video_id}, will calculate from video_id")
            num_frames = calculate_num_frames(video_id)
    else:
        # Fallback to calculating from video_id
        num_frames = calculate_num_frames(video_id)

    # Use the same subfolder for all related files
    rel_video_path = os.path.join(download_base_dir, 'video', subfolder, os.path.basename(video_path))
    local_video_path = os.path.abspath(rel_video_path)
    
    # Prepare results dictionary
    results = {
        'video_id': video_id,
        'actor_id': actor_id,
        'quality_score': quality_score,
        'subfolder': subfolder,
        'num_frames': num_frames,
        'local_audio_path': None,
        'local_landmark_path': None
    }
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Video download is mandatory
        video_future = executor.submit(download_from_s3, video_path, local_video_path, s3_client, logger)
        
        # Set up optional downloads
        audio_future = None
        landmark_future = None
        
        if audio_path:
            rel_audio_path = os.path.join(download_base_dir, 'audio', subfolder, os.path.basename(audio_path))
            local_audio_path = os.path.abspath(rel_audio_path)
            audio_future = executor.submit(download_from_s3, audio_path, local_audio_path, s3_client, logger)
        
        if landmarks_path:
            rel_landmark_path = os.path.join(download_base_dir, 'landmarks', subfolder, os.path.basename(landmarks_path))
            local_landmark_path = os.path.abspath(rel_landmark_path)
            landmark_future = executor.submit(download_from_s3, landmarks_path, local_landmark_path, s3_client, logger)
        
        # Get results from futures
        results['local_video_path'] = video_future.result()
        
        if audio_future:
            results['local_audio_path'] = audio_future.result()
        
        if landmark_future:
            results['local_landmark_path'] = landmark_future.result()
    
    return results

def process_dynamo_entries(table_name, download_base_dir, region, workers, quality_threshold, logger):
    """Process entries from DynamoDB table."""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    
    table = dynamodb.Table(table_name)
    results = []
    
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
    
    # Filter entries based on quality score
    filtered_entries = [item for item in items if float(item.get('quality_score', 0.0)) >= quality_threshold]
    
    logger.info(f"Filtered to {len(filtered_entries)} entries with quality score >= {quality_threshold}")
    
    # Process entries with organized folder structure
    for i, entry in enumerate(filtered_entries):
        if 'video_path' not in entry or not entry['video_path']:
            logger.warning(f"Skipping entry {entry.get('video_id', 'unknown')} - missing video path")
            continue
            
        # Calculate which subfolder this file should go in
        subfolder = get_subfolder_number(i)
        result = download_files(entry, download_base_dir, subfolder, s3, workers, logger)
        
        # Only add to results if video was successfully downloaded
        if result['local_video_path']:
            results.append(result)
        else:
            logger.warning(f"Skipping entry {entry['video_id']} - video download failed")
    
    return results

def create_folder_structure(download_base_dir, logger):
    """Create the base folder structure for downloads."""
    # Create base directories for each type
    base_dirs = ['video', 'audio', 'landmarks']
    
    for base_dir in base_dirs:
        dir_path = os.path.join(download_base_dir, base_dir)
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def write_to_csv(results, output_file, logger):
    """Write results to CSV file."""
    if not results:
        logger.warning("No results to write to CSV")
        return
    
    fieldnames = ['video_id', 'actor_id', 'subfolder', 'local_video_path', 'local_audio_path', 
                 'local_landmark_path', 'quality_score', 'num_frames']
    
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    logger.info(f"Wrote {len(results)} entries to {output_file}")

def main():
    """Main function to process DynamoDB entries and create CSV."""
    args = parse_arguments()
    
    logger = setup_logging(args.log_level)
    
    logger.info(f"Starting DynamoDB processing with table '{args.table}'")
    logger.info(f"Quality threshold set to {args.quality_threshold}")
    
    # Create the initial folder structure
    download_base_dir = args.download_dir
    create_folder_structure(download_base_dir, logger)
    
    results = process_dynamo_entries(args.table, download_base_dir, args.region, 
                                    args.workers, args.quality_threshold, logger)
    
    write_to_csv(results, args.output_csv, logger)
    
    logger.info("Processing complete")

if __name__ == "__main__":
    main()