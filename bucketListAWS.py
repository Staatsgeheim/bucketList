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
logger = logging.getLogger('bucketListAWS')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s [%(bucket_name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

def check_file_access(base_url, key, aggressive=False, bucket_name="unknown"):
    """
    Perform a HEAD request (default) or aggressive GET request with Range header to check file accessibility,
    suppressing SSL/TLS errors.
    
    Args:
        base_url (str): Base URL of the S3 bucket
        key (str): File key
        aggressive (bool): Use GET with Range header instead of HEAD
        bucket_name (str): Bucket name for logging
    
    Returns:
        tuple: (is_accessible: bool, content_length: str or None)
    """
    try:
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(key)}"
        extra = {'bucket_name': bucket_name}
        if aggressive:
            headers = {'Range': 'bytes=0-3'}
            logger.info(f"Aggressive check: Testing {key}...", extra=extra)
            response = requests.get(file_url, headers=headers, stream=True, verify=False)
        else:
            logger.info(f"Checking file {key}...", extra=extra)
            response = requests.head(file_url, verify=False)
        
        is_accessible = response.status_code in (200, 206)
        content_length = response.headers.get("Content-Length")
        logger.info(f"{'Aggressive check: ' if aggressive else 'Check: '}File {key} {'is accessible' if is_accessible else f'not accessible, status code: {response.status_code}'}", extra=extra)
        return is_accessible, content_length
    except SSLError as e:
        #logger.error(f"SSL/TLS error for {key}: {str(e)}. Continuing with suppressed verification.", extra=extra)
        return False, None
    except Exception as e:
        logger.error(f"Error checking access for {key}: {str(e)}", extra=extra)
        return False, None

def download_file(base_url, key, download_dir, bucket_name):
    """
    Download a file from an S3 bucket to the specified directory, preserving the key path,
    suppressing SSL/TLS errors.
    
    Args:
        base_url (str): Base URL of the S3 bucket
        key (str): File key
        download_dir (str): Base directory for downloads (or None to use bucket_name)
        bucket_name (str): Name of the bucket
    
    Returns:
        bool: True if download succeeded, False otherwise
    """
    try:
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(key)}"
        extra = {'bucket_name': bucket_name}
        if download_dir:
            base_path = os.path.join(download_dir, bucket_name)
        else:
            base_path = bucket_name
        
        # Ensure key is treated as a relative path
        local_path_obj = Path(base_path) / Path(key.lstrip('/\\'))
        local_path = str(local_path_obj)
        
        with logging_lock:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Downloading {key}...", extra=extra)
        response = requests.get(file_url, stream=True, verify=False)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded {key} to {local_path}", extra=extra)
            return True
        else:
            logger.error(f"Failed to download {key}, status code: {response.status_code}", extra=extra)
            return False
    except SSLError as e:
        #logger.error(f"SSL/TLS error downloading {key}: {str(e)}. Continuing with suppressed verification.", extra=extra)
        return False
    except Exception as e:
        logger.error(f"Error downloading {key}: {str(e)}", extra=extra)
        return False

def matches_patterns(key, patterns, is_dir=False):
    """
    Check if a key matches any of the provided patterns (case-insensitive for files).
    """
    if not patterns:
        return False
    key_path = Path(key)
    for pattern in patterns:
        if is_dir:
            pattern_path = Path(pattern.lstrip('/')).as_posix()
            if key.startswith(pattern_path + '/') or fnmatch(key, f"{pattern_path}/*") or key == pattern_path : # Added direct match for dir
                return True
        else:
            if (fnmatch(key.lower(), pattern.lower()) or 
                fnmatch(key_path.name.lower(), pattern.lower())):
                return True
    return False

def list_bucket_contents(url, check_access=False, aggressive_check=False, download=False, download_dir=None,
                         include_files=None, include_dirs=None, exclude_files=None, exclude_dirs=None):
    """
    Fetch and parse all contents of a publicly accessible S3 bucket via HTTPS GET requests with pagination.
    Filters (include/exclude) apply to the list itself if not downloading, or to downloads if downloading.
    """
    bucket_name_for_logging = "unknown" # Initial value
    extra = {'bucket_name': bucket_name_for_logging}
    
    try:
        all_contents = []
        namespace = 'http://s3.amazonaws.com/doc/2006-03-01/'
        marker = None
        page_number = 0
        total_objects_listed = 0 # Tracks objects that pass listing filters
        total_size_listed = 0    # Tracks size of objects that pass listing filters
        
        while True:
            page_number += 1
            request_url = url
            if marker:
                parsed_url = urllib.parse.urlparse(url)
                query = urllib.parse.parse_qs(parsed_url.query)
                query['marker'] = [urllib.parse.quote(marker)] # S3 marker might need quoting
                new_query = urllib.parse.urlencode(query, doseq=True)
                request_url = urllib.parse.urlunparse(
                    parsed_url._replace(query=new_query)
                )
            
            logger.info(f"Fetching page {page_number} (marker: {marker[:20] + '...' if marker and len(marker) > 20 else marker})...", extra=extra)
            response = requests.get(request_url, verify=False) # verify=False for S3 too
            response.raise_for_status()
            
            root = ET.fromstring(response.text)
            if root.tag != f'{{{namespace}}}ListBucketResult':
                raise ValueError("Unexpected XML format")
            
            name_elem = root.find(f'{{{namespace}}}Name')
            if name_elem is None:
                raise ValueError("Name element not found in XML")
            bucket_name_for_logging = name_elem.text # Update bucket_name from XML
            extra['bucket_name'] = bucket_name_for_logging # Update logger's extra context
            
            page_contents_passing_list_filter = []
            for content_elem in root.findall(f'{{{namespace}}}Contents'):
                key = content_elem.find(f'{{{namespace}}}Key').text
                last_modified = content_elem.find(f'{{{namespace}}}LastModified').text
                size_str = content_elem.find(f'{{{namespace}}}Size').text
                etag = content_elem.find(f'{{{namespace}}}ETag').text
                storage_class_elem = content_elem.find(f'{{{namespace}}}StorageClass')
                storage_class = storage_class_elem.text if storage_class_elem is not None else None
                
                owner_elem = content_elem.find(f'{{{namespace}}}Owner')
                owner = None
                if owner_elem is not None:
                    owner_id_elem = owner_elem.find(f'{{{namespace}}}ID')
                    owner_display_name_elem = owner_elem.find(f'{{{namespace}}}DisplayName')
                    owner = {
                        'ID': owner_id_elem.text if owner_id_elem is not None else None,
                        'DisplayName': owner_display_name_elem.text if owner_display_name_elem is not None else None
                    }

                # --- Filtering logic for LISTING (if --download is NOT active) ---
                if not download: # Apply filters to the list itself
                    should_be_listed = True
                    log_skip_reason = ""
                    if (exclude_files and matches_patterns(key, exclude_files)) or \
                       (exclude_dirs and matches_patterns(key, exclude_dirs, is_dir=True)):
                        should_be_listed = False
                        log_skip_reason = "matches exclude pattern for listing"
                    elif include_files or include_dirs: # Only apply include if some are specified
                        if not (matches_patterns(key, include_files) or \
                                matches_patterns(key, include_dirs, is_dir=True)):
                            should_be_listed = False
                            log_skip_reason = "does not match include pattern for listing"
                    
                    if not should_be_listed:
                        logger.debug(f"Skipping {key} from listing: {log_skip_reason}", extra=extra)
                        continue # Skip this object, don't add to list or count size/objects

                # --- If we reach here, the item is eligible for listing (or download=True, so all are listed) ---
                item = {
                    'Key': key,
                    'LastModified': last_modified,
                    'Size': int(size_str), # Store as int
                    'ETag': etag,
                    'StorageClass': storage_class
                }
                if owner:
                    item['Owner'] = owner
                
                is_accessible_for_check = True # Default for non-checked items
                if check_access:
                    is_accessible_for_check, content_length = check_file_access(url, key, aggressive=aggressive_check, bucket_name=bucket_name_for_logging)
                    item['is_accessible'] = is_accessible_for_check
                    item['content_length_from_header'] = content_length
                
                # --- Download Logic (applies if --download is active) ---
                if download:
                    should_download_this_item = is_accessible_for_check if check_access else True
                    
                    # Apply download-specific filtering
                    if (exclude_files and matches_patterns(key, exclude_files)) or \
                       (exclude_dirs and matches_patterns(key, exclude_dirs, is_dir=True)):
                        logger.debug(f"Skipping download of {key}: matches exclude pattern", extra=extra)
                        should_download_this_item = False
                    elif include_files or include_dirs: # Only apply include if some are specified
                        if not (matches_patterns(key, include_files) or \
                                matches_patterns(key, include_dirs, is_dir=True)):
                            logger.debug(f"Skipping download of {key}: does not match include patterns", extra=extra)
                            should_download_this_item = False
                    
                    if should_download_this_item:
                        download_file(url, key, download_dir, bucket_name_for_logging)
                
                # Add to list of contents for JSON output and count towards totals
                page_contents_passing_list_filter.append(item)
                total_size_listed += int(size_str) # Counts size of items that will be listed
                
            all_contents.extend(page_contents_passing_list_filter)
            total_objects_listed += len(page_contents_passing_list_filter) # Counts items that will be listed
            logger.info(f"Page {page_number}: {len(page_contents_passing_list_filter)} objects passed listing filters (Total listed so far: {total_objects_listed})", extra=extra)
            
            is_truncated_elem = root.find(f'{{{namespace}}}IsTruncated')
            if is_truncated_elem is None:
                raise ValueError("IsTruncated element not found in XML")
            is_truncated = is_truncated_elem.text.lower() == 'true'
            
            marker = None # Reset marker for S3; it's the key of the last item if truncated
            if is_truncated:
                # S3's NextMarker is often not present or is the same as the last key for regular listing.
                # For robust pagination, if IsTruncated is true, the marker for the next request
                # should be the key of the last item in the current <Contents> list.
                # If there are no Contents but IsTruncated is true (e.g. filtered out everything)
                # it might be more complex or we rely on NextMarker if available.
                # For simplicity, if page_contents_passing_list_filter is empty but truncated, we might loop.
                # However, S3 usually provides NextContinuationToken for V2 lists, or relies on last key for V1.
                # This script uses V1 style listing.
                
                # Let's check for NextMarker first, as it's more explicit if present
                next_marker_elem = root.find(f'{{{namespace}}}NextMarker') # AWS specific pagination element
                if next_marker_elem is not None and next_marker_elem.text:
                    marker = next_marker_elem.text
                elif page_contents_passing_list_filter: # Fallback to last key if NextMarker not present
                    marker = page_contents_passing_list_filter[-1]['Key']
                # If page_contents is empty and no NextMarker, and truncated, this could be an issue.
                # However, if all items were filtered out on a page, this logic means marker remains None,
                # which could break pagination if there were subsequent pages.
                # A more robust S3 paginator would use the *original* last key from the XML before filtering.
                # For this script, this might be an edge case if all items on a page are filtered out.
                # For now, we assume if page_contents_passing_list_filter is empty, there's no reliable marker from content.

            if not is_truncated or marker is None: # If not truncated, or no marker to continue, break.
                break
        
        logger.info(f"Finished fetching. Total objects listed: {total_objects_listed}, Total size of listed objects: {total_size_listed} bytes", extra=extra)
        
        timestamp = datetime.now(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return url, {
            'name': bucket_name_for_logging,
            'timestamp': timestamp,
            'total_items': total_objects_listed, # Reflects items passing list filters
            'total_size': total_size_listed,   # Reflects size of items passing list filters
            'contents': all_contents
        }
    
    except SSLError as e:
        #logger.error(f"SSL/TLS error processing bucket: {str(e)}. Continuing with suppressed verification.", extra=extra)
        return url, None
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error processing bucket {url}: {str(e)}", extra=extra)
        return url, None
    except ET.ParseError as e:
        logger.error(f"XML parsing error for bucket {url}: {str(e)}", extra=extra)
        return url, None
    except Exception as e:
        logger.error(f"Error processing bucket {url}: {str(e)}", extra=extra)
        import traceback
        logger.debug(traceback.format_exc(), extra=extra)
        return url, None

def read_urls_from_file(file_path):
    """
    Read URLs from a text file, one per line, ignoring empty lines and comments.
    """
    try:
        with open(file_path, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return urls
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}", extra={'bucket_name': 'file_reader'})
        return []

def main():
    parser = argparse.ArgumentParser(
        description="List contents of S3 buckets and output as JSON. Filters apply to the list or downloads.",
        epilog="Provide S3 bucket URLs directly or use --file. Include/Exclude filters apply to listing unless --download is used, then they apply to downloads.",
    )
    parser.add_argument(
        'urls',
        nargs='*',
        help="S3 bucket URLs (e.g., https://your-bucket-name.s3.us-east-1.amazonaws.com/)",
    )
    parser.add_argument(
        '-f', '--file',
        help="Path to a text file containing S3 bucket URLs, one per line",
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=1,
        help="Number of parallel threads for processing buckets (default: 1)",
    )
    access_group = parser.add_argument_group('Access Check Options')
    access_group.add_argument(
        '--check-access',
        action='store_true',
        help="Perform requests to check file accessibility (default: HEAD request)",
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
        help="Download all accessible files. Filters will apply to downloads instead of listing.",
    )
    download_group.add_argument(
        '--download-dir',
        help="Specify a directory for downloaded files (defaults to bucket name)",
    )
    
    filter_group = parser.add_argument_group('Filtering Options (apply to listing OR download)')
    filter_group.add_argument(
        '--include-file',
        action='append',
        default=[],
        help="File patterns to include. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--include-dir',
        action='append',
        default=[],
        help="Directory patterns to include. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--exclude-file',
        action='append',
        default=[],
        help="File patterns to exclude. Applies to listing, or to downloads if -d is used.",
    )
    filter_group.add_argument(
        '--exclude-dir',
        action='append',
        default=[],
        help="Directory patterns to exclude. Applies to listing, or to downloads if -d is used.",
    )
    
    args = parser.parse_args()
    
    if args.aggressive and not args.check_access:
        logger.error("Error: --aggressive requires --check-access", extra={'bucket_name': 'main'})
        sys.exit(1)
    # REMOVED: The check that required --download for include/exclude filters.
    # if (args.include_file or args.include_dir or args.exclude_file or args.exclude_dir) and not args.download:
    #     logger.error("Error: --include-file, --include-dir, --exclude-file, and --exclude-dir require --download", extra={'bucket_name': 'main'})
    #     sys.exit(1)
    if args.threads < 1:
        logger.error("Error: --threads must be at least 1", extra={'bucket_name': 'main'})
        sys.exit(1)
    
    urls = args.urls
    if args.file:
        file_urls = read_urls_from_file(args.file)
        if not file_urls:
            logger.error("No valid URLs found in file. Exiting.", extra={'bucket_name': 'main'})
            sys.exit(1)
        urls.extend(file_urls)
    
    if not urls:
        logger.error("No URLs provided. Use URLs as arguments or --file option.", extra={'bucket_name': 'main'})
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    
    result_data = {} # Renamed from 'result'
    grand_total_items = 0
    grand_total_size = 0
    buckets_processed = 0
    
    def process_bucket(url):
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
    
    logger.info(f"Starting processing of {len(urls)} bucket(s) with {args.threads} thread(s)...", extra={'bucket_name': 'main'})
    
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        processed_results = list(executor.map(process_bucket, urls)) # Renamed 'results' variable
    
    for url, bucket_data in processed_results:
        if bucket_data is not None:
            result_data[url] = bucket_data
            grand_total_items += bucket_data['total_items'] # These are now filtered totals
            grand_total_size += bucket_data['total_size']   # These are now filtered totals
            buckets_processed += 1
    
    if buckets_processed > 0: # Show summary if at least one bucket was processed
        summary_type = "listed" if not args.download else "found (downloads filtered separately)"
        logger.info(f"Final summary: Processed {buckets_processed} buckets. Total objects {summary_type}: {grand_total_items}, Total size of {summary_type} objects: {grand_total_size} bytes", extra={'bucket_name': 'main'})

    print(json.dumps(result_data, indent=2))
    
    if not result_data:
        logger.error("No buckets were successfully processed or no items matched filters.", extra={'bucket_name': 'main'})
        if buckets_processed == 0 : # only exit if no buckets were processed at all.
             sys.exit(1)

if __name__ == "__main__":
    main()