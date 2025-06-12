"""
MIT License

Copyright (c) 2025 Maarten Boone

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import requests
import xml.etree.ElementTree as ET
import sys
import urllib.parse
import json
import argparse
import os
import logging
from fnmatch import fnmatch
from pathlib import Path
from datetime import datetime
import pytz
import warnings
from requests.exceptions import SSLError
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Thread-safe logging setup
logging_lock = Lock()
logger = logging.getLogger('bucketListGoogle') 
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
# Kept %(bucket_name)s for simplicity as it's a common term, or change to %(gcs_bucket_name)s if preferred
formatter = logging.Formatter('%(asctime)s [%(bucket_name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

def check_file_access(base_url, object_name, aggressive=False, bucket_name="unknown"):
    """
    Perform a HEAD request (default) or aggressive GET request with Range header to check GCS object accessibility,
    suppressing SSL/TLS errors.
    
    Args:
        base_url (str): Base URL of the GCS bucket (e.g., https://storage.googleapis.com/BUCKET_NAME)
        object_name (str): Object name (key)
        aggressive (bool): Use GET with Range header instead of HEAD
        bucket_name (str): Bucket name for logging
    
    Returns:
        tuple: (is_accessible: bool, content_length: str or None)
    """
    try:
        # Construct GCS object URL: base_url should be like https://storage.googleapis.com/BUCKET or https://BUCKET.storage.googleapis.com
        # urllib.parse.quote is important for object names with special characters.
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(object_name)}"
        extra = {'bucket_name': bucket_name}
        if aggressive:
            headers = {'Range': 'bytes=0-3'}
            logger.info(f"Aggressive check: Testing GCS object {object_name}...", extra=extra)
            response = requests.get(file_url, headers=headers, stream=True, verify=False)
        else:
            logger.info(f"Checking GCS object {object_name}...", extra=extra)
            response = requests.head(file_url, verify=False)
        
        is_accessible = response.status_code in (200, 206)
        content_length = response.headers.get("Content-Length")
        logger.info(f"{'Aggressive check: ' if aggressive else 'Check: '}GCS object {object_name} {'is accessible' if is_accessible else f'not accessible, status code: {response.status_code}'}", extra=extra)
        return is_accessible, content_length
    except SSLError as e:
        return False, None
    except Exception as e:
        logger.error(f"Error checking access for GCS object {object_name}: {str(e)}", extra=extra)
        return False, None

def download_file(base_url, object_name, download_dir, bucket_name_for_path):
    """
    Download an object from a GCS bucket to the specified directory, preserving the object name path,
    suppressing SSL/TLS errors.
    
    Args:
        base_url (str): Base URL of the GCS bucket
        object_name (str): Object name (key)
        download_dir (str): Base directory for downloads (or None to use bucket_name_for_path)
        bucket_name_for_path (str): Name of the bucket (used for subfolder)
    
    Returns:
        bool: True if download succeeded, False otherwise
    """
    try:
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(object_name)}"
        extra = {'bucket_name': bucket_name_for_path}
        if download_dir:
            base_path = os.path.join(download_dir, bucket_name_for_path)
        else:
            base_path = bucket_name_for_path
        
        local_path_obj = Path(base_path) / Path(object_name.lstrip('/\\'))
        local_path = str(local_path_obj)
        
        with logging_lock:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Downloading GCS object {object_name}...", extra=extra)
        response = requests.get(file_url, stream=True, verify=False)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded GCS object {object_name} to {local_path}", extra=extra)
            return True
        else:
            logger.error(f"Failed to download GCS object {object_name}, status code: {response.status_code}", extra=extra)
            return False
    except SSLError as e:
        return False
    except Exception as e:
        logger.error(f"Error downloading GCS object {object_name}: {str(e)}", extra=extra)
        return False

def matches_patterns(key, patterns, is_dir=False):
    """
    Check if a key matches any of the provided patterns (case-insensitive for files).
    (This function is generic and remains unchanged)
    """
    if not patterns:
        return False
    key_path = Path(key)
    for pattern in patterns:
        if is_dir:
            pattern_path = Path(pattern.lstrip('/')).as_posix()
            if key.startswith(pattern_path + '/') or fnmatch(key, f"{pattern_path}/*") or key == pattern_path:
                return True
        else:
            if (fnmatch(key.lower(), pattern.lower()) or 
                fnmatch(key_path.name.lower(), pattern.lower())):
                return True
    return False

def list_bucket_contents(url, check_access=False, aggressive_check=False, download=False, download_dir=None,
                         include_files=None, include_dirs=None, exclude_files=None, exclude_dirs=None):
    """
    Fetch and parse all contents of a publicly accessible GCS bucket via HTTPS GET requests with pagination.
    Filters (include/exclude) apply to the list itself if not downloading, or to downloads if downloading.
    GCS XML API is S3 compatible.
    """
    bucket_name_for_logging = "unknown_gcs_bucket" # Initial value
    extra = {'bucket_name': bucket_name_for_logging}
    
    try:
        all_contents = []
        # GCS uses the S3 XML namespace for compatibility
        namespace = 'http://doc.s3.amazonaws.com/2006-03-01' 
        marker = None
        page_number = 0
        total_objects_listed = 0 
        total_size_listed = 0    
        
        # The input URL is the base for listing and object access
        bucket_base_url = url.rstrip('/')


        while True:
            page_number += 1
            request_url = bucket_base_url # For GCS, listing is usually on the base bucket URL
            
            # Add query parameters for pagination
            params = {}
            if marker:
                params['marker'] = marker 
            # params['max-keys'] = 1000 # Optionally control page size, GCS default is 1000

            logger.info(f"Fetching page {page_number} (marker: {marker[:20] + '...' if marker and len(marker) > 20 else marker})...", extra=extra)
            response = requests.get(request_url, params=params, verify=False)
            response.raise_for_status()
            
            xml_text = response.text
            root = ET.fromstring(xml_text)
            if root.tag != f'{{{namespace}}}ListBucketResult':
                # Attempt to find it if it's nested (should not be for GCS/S3 direct response)
                lbr_element = root.find(f'.//{{{namespace}}}ListBucketResult')
                if lbr_element is not None:
                    root = lbr_element
                else:
                    raise ValueError(f"Unexpected XML format: root tag is {root.tag}, not ListBucketResult")

            name_elem = root.find(f'{{{namespace}}}Name')
            if name_elem is None:
                raise ValueError("Bucket Name element not found in XML")
            bucket_name_for_logging = name_elem.text 
            extra['bucket_name'] = bucket_name_for_logging
            
            page_contents_passing_list_filter = []
            for content_elem in root.findall(f'{{{namespace}}}Contents'):
                key_elem = content_elem.find(f'{{{namespace}}}Key')
                last_modified_elem = content_elem.find(f'{{{namespace}}}LastModified')
                size_elem = content_elem.find(f'{{{namespace}}}Size')
                etag_elem = content_elem.find(f'{{{namespace}}}ETag')
                storage_class_elem = content_elem.find(f'{{{namespace}}}StorageClass') # GCS has this
                
                if key_elem is None or last_modified_elem is None or size_elem is None or etag_elem is None:
                    logger.warning("Skipping malformed content entry in GCS XML", extra=extra)
                    continue

                key = key_elem.text
                last_modified = last_modified_elem.text
                size_str = size_elem.text
                etag = etag_elem.text
                storage_class = storage_class_elem.text if storage_class_elem is not None else None
                
                owner_elem = content_elem.find(f'{{{namespace}}}Owner') # GCS may include this
                owner = None
                if owner_elem is not None:
                    owner_id_elem = owner_elem.find(f'{{{namespace}}}ID')
                    owner_display_name_elem = owner_elem.find(f'{{{namespace}}}DisplayName')
                    owner = {
                        'ID': owner_id_elem.text if owner_id_elem is not None else None,
                        'DisplayName': owner_display_name_elem.text if owner_display_name_elem is not None else None
                    }

                if not download: 
                    should_be_listed = True
                    log_skip_reason = ""
                    if (exclude_files and matches_patterns(key, exclude_files)) or \
                       (exclude_dirs and matches_patterns(key, exclude_dirs, is_dir=True)):
                        should_be_listed = False
                        log_skip_reason = "matches exclude pattern for listing"
                    elif include_files or include_dirs: 
                        if not (matches_patterns(key, include_files) or \
                                matches_patterns(key, include_dirs, is_dir=True)):
                            should_be_listed = False
                            log_skip_reason = "does not match include pattern for listing"
                    
                    if not should_be_listed:
                        logger.info(f"Skipping GCS object {key} from listing: {log_skip_reason}", extra=extra)
                        continue

                item = {
                    'Key': key,
                    'LastModified': last_modified,
                    'Size': int(size_str), 
                    'ETag': etag,
                    'StorageClass': storage_class # GCS specific values like STANDARD, NEARLINE etc.
                }
                if owner:
                    item['Owner'] = owner
                
                is_accessible_for_check = True 
                if check_access:
                    # Use bucket_base_url for checking individual files
                    is_accessible_for_check, content_length = check_file_access(bucket_base_url, key, aggressive=aggressive_check, bucket_name=bucket_name_for_logging)
                    item['is_accessible'] = is_accessible_for_check
                    item['content_length_from_header'] = content_length
                
                if download:
                    should_download_this_item = is_accessible_for_check if check_access else True
                    
                    if (exclude_files and matches_patterns(key, exclude_files)) or \
                       (exclude_dirs and matches_patterns(key, exclude_dirs, is_dir=True)):
                        logger.info(f"Skipping download of GCS object {key}: matches exclude pattern", extra=extra)
                        should_download_this_item = False
                    elif include_files or include_dirs: 
                        if not (matches_patterns(key, include_files) or \
                                matches_patterns(key, include_dirs, is_dir=True)):
                            logger.info(f"Skipping download of GCS object {key}: does not match include patterns", extra=extra)
                            should_download_this_item = False
                    
                    if should_download_this_item:
                        download_file(bucket_base_url, key, download_dir, bucket_name_for_logging)
                
                page_contents_passing_list_filter.append(item)
                total_size_listed += int(size_str)
                
            all_contents.extend(page_contents_passing_list_filter)
            total_objects_listed += len(page_contents_passing_list_filter)
            logger.info(f"Page {page_number}: {len(page_contents_passing_list_filter)} GCS objects passed listing filters (Total listed so far: {total_objects_listed})", extra=extra)
            
            is_truncated_elem = root.find(f'{{{namespace}}}IsTruncated')
            if is_truncated_elem is None:
                raise ValueError("IsTruncated element not found in GCS XML")
            is_truncated = is_truncated_elem.text.lower() == 'true'
            
            marker = None 
            if is_truncated:
                # GCS XML API typically provides NextMarker when truncated
                next_marker_elem = root.find(f'{{{namespace}}}NextMarker') 
                if next_marker_elem is not None and next_marker_elem.text:
                    marker = next_marker_elem.text
                elif page_contents_passing_list_filter: # Fallback like S3 if NextMarker isn't there
                    marker = page_contents_passing_list_filter[-1]['Key']
                # If NextMarker is absent and page_contents_passing_list_filter is empty, marker remains None.
            
            if not is_truncated or marker is None:
                break
        
        logger.info(f"Finished fetching GCS bucket. Total objects listed: {total_objects_listed}, Total size of listed objects: {total_size_listed} bytes", extra=extra)
        
        timestamp = datetime.now(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return url, {
            'name': bucket_name_for_logging,
            'timestamp': timestamp,
            'total_items': total_objects_listed, 
            'total_size': total_size_listed,   
            'contents': all_contents
        }
    
    except SSLError as e:
        return url, None
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error processing GCS bucket {url}: {str(e)}", extra=extra)
        return url, None
    except ET.ParseError as e:
        logger.error(f"XML parsing error for GCS bucket {url}: {str(e)}", extra=extra)
        logger.debug(f"Problematic XML content for GCS {url}:\n{xml_text[:1000] if 'xml_text' in locals() else 'XML not available'}", extra=extra)
        return url, None
    except Exception as e:
        logger.error(f"Error processing GCS bucket {url}: {str(e)}", extra=extra)
        import traceback
        logger.debug(traceback.format_exc(), extra=extra)
        return url, None

def read_urls_from_file(file_path):
    """
    Read URLs from a text file. (Generic function)
    """
    try:
        with open(file_path, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return urls
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}", extra={'bucket_name': 'file_reader'}) # Using generic bucket_name
        return []

def main():
    parser = argparse.ArgumentParser(
        description="List contents of Google Cloud Storage (GCS) buckets and output as JSON. Filters apply to the list or downloads.",
        epilog="Provide GCS bucket URLs (e.g., https://BUCKET_NAME.storage.googleapis.com/ or https://storage.googleapis.com/BUCKET_NAME/) or use --file. Include/Exclude filters apply to listing unless --download is used, then they apply to downloads.",
    )
    parser.add_argument(
        'urls',
        nargs='*',
        help="GCS bucket URLs",
    )
    parser.add_argument(
        '-f', '--file',
        help="Path to a text file containing GCS bucket URLs, one per line",
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=1,
        help="Number of parallel threads for processing GCS buckets (default: 1)",
    )
    access_group = parser.add_argument_group('Access Check Options')
    access_group.add_argument(
        '--check-access',
        action='store_true',
        help="Perform requests to check GCS object accessibility (default: HEAD request)",
    )
    access_group.add_argument(
        '--aggressive',
        action='store_true',
        help="Use aggressive GET request with Range header for access checks (requires --check-access)",
    )
    download_group = parser.add_argument_group('Download Options')
    download_group.add_argument(
        '-d', '--download',
        action='store_true',
        help="Download all accessible GCS objects. Filters will apply to downloads instead of listing.",
    )
    download_group.add_argument(
        '--download-dir',
        help="Specify a directory for downloaded GCS objects (defaults to a subfolder named after the bucket)",
    )
    
    filter_group = parser.add_argument_group('Filtering Options (apply to listing OR download)')
    filter_group.add_argument(
        '--include-file',
        action='append',
        default=[],
        help="GCS object name patterns to include. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--include-dir',
        action='append',
        default=[],
        help="GCS object path (directory) patterns to include. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--exclude-file',
        action='append',
        default=[],
        help="GCS object name patterns to exclude. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--exclude-dir',
        action='append',
        default=[],
        help="GCS object path (directory) patterns to exclude. Applies to listing, or to downloads if -d is used.",
    )
    
    args = parser.parse_args()
    
    if args.aggressive and not args.check_access:
        logger.error("Error: --aggressive requires --check-access", extra={'bucket_name': 'main'})
        sys.exit(1)
    if args.threads < 1:
        logger.error("Error: --threads must be at least 1", extra={'bucket_name': 'main'})
        sys.exit(1)
    
    urls = args.urls
    if args.file:
        file_urls = read_urls_from_file(args.file)
        if not file_urls:
            logger.error("No valid GCS URLs found in file. Exiting.", extra={'bucket_name': 'main'})
            sys.exit(1)
        urls.extend(file_urls)
    
    if not urls:
        logger.error("No GCS URLs provided. Use URLs as arguments or --file option.", extra={'bucket_name': 'main'})
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    
    result_data = {}
    grand_total_items = 0
    grand_total_size = 0
    buckets_processed = 0
    
    def process_gcs_bucket(url): # Renamed function for clarity
        return list_bucket_contents(
            url,
            check_access=args.check_access,
            aggressive_check=args.aggressive,
            download=args.download,
            download_dir=args.download_dir,
            include_files=args.include_file,
            include_dirs=args.include_dir,
            exclude_files=args.exclude_file,
            exclude_dirs=args.exclude_dir
        )
    
    logger.info(f"Starting processing of {len(urls)} GCS bucket(s) with {args.threads} thread(s)...", extra={'bucket_name': 'main'})
    
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        processed_results = list(executor.map(process_gcs_bucket, urls))
    
    for url, bucket_data in processed_results:
        if bucket_data is not None:
            result_data[url] = bucket_data
            grand_total_items += bucket_data['total_items']
            grand_total_size += bucket_data['total_size']
            buckets_processed += 1
    
    if buckets_processed > 0:
        summary_type = "listed" if not args.download else "found (downloads filtered separately)"
        logger.info(f"Final summary: Processed {buckets_processed} GCS buckets. Total objects {summary_type}: {grand_total_items}, Total size of {summary_type} objects: {grand_total_size} bytes", extra={'bucket_name': 'main'})

    print(json.dumps(result_data, indent=2))
    
    if not result_data:
        logger.error("No GCS buckets were successfully processed or no objects matched filters.", extra={'bucket_name': 'main'})
        if buckets_processed == 0 :
             sys.exit(1)

if __name__ == "__main__":
    main()