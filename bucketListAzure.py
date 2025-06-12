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
from datetime import datetime, timezone
import pytz
import warnings
from requests.exceptions import SSLError
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Thread-safe logging setup
logging_lock = Lock()
logger = logging.getLogger('bucketListAzure')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s [%(container_name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_container_name_from_url(url_str):
    """Helper to get container name from Azure Blob Storage URL."""
    try:
        parsed_url = urllib.parse.urlparse(url_str)
        name = parsed_url.path.lstrip('/')
        if not name:
            host_parts = parsed_url.hostname.split('.')
            if len(host_parts) > 1 and host_parts[1] == 'blob':
                return host_parts[0]
            return "unknown_container"
        return name
    except Exception:
        return "unknown_container"

def check_file_access(base_url, blob_name, aggressive=False, container_name="unknown"):
    """
    Perform a HEAD request (default) or aggressive GET request with Range header to check blob accessibility,
    suppressing SSL/TLS errors.
    """
    try:
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(blob_name)}"
        extra = {'container_name': container_name}
        if aggressive:
            headers = {'Range': 'bytes=0-3'}
            logger.info(f"Aggressive check: Testing {blob_name}...", extra=extra)
            response = requests.get(file_url, headers=headers, stream=True, verify=False)
        else:
            logger.info(f"Checking blob {blob_name}...", extra=extra)
            response = requests.head(file_url, verify=False)
        
        is_accessible = response.status_code in (200, 206)
        content_length = response.headers.get("Content-Length")
        logger.info(f"{'Aggressive check: ' if aggressive else 'Check: '}Blob {blob_name} {'is accessible' if is_accessible else f'not accessible, status code: {response.status_code}'}", extra=extra)
        return is_accessible, content_length
    except SSLError:
        return False, None
    except Exception as e:
        logger.error(f"Error checking access for {blob_name}: {str(e)}", extra=extra)
        return False, None

def download_file(base_url, blob_name, download_dir, container_name_for_path):
    """
    Download a blob from an Azure container to the specified directory, preserving the blob name path,
    suppressing SSL/TLS errors.
    """
    try:
        file_url = f"{base_url.rstrip('/')}/{urllib.parse.quote(blob_name)}"
        extra = {'container_name': container_name_for_path}
        if download_dir:
            base_path = os.path.join(download_dir, container_name_for_path)
        else:
            base_path = container_name_for_path
        
        local_path_obj = Path(base_path) / Path(blob_name.lstrip('/\\'))
        local_path = str(local_path_obj)

        with logging_lock:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Downloading {blob_name}...", extra=extra)
        response = requests.get(file_url, stream=True, verify=False)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded {blob_name} to {local_path}", extra=extra)
            return True
        else:
            logger.error(f"Failed to download {blob_name}, status code: {response.status_code}", extra=extra)
            return False
    except SSLError:
        return False
    except Exception as e:
        logger.error(f"Error downloading {blob_name}: {str(e)}", extra=extra)
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
            if key.startswith(pattern_path + '/') or fnmatch(key, f"{pattern_path}/*") or key == pattern_path:
                return True
        else:
            if (fnmatch(key.lower(), pattern.lower()) or 
                fnmatch(key_path.name.lower(), pattern.lower())):
                return True
    return False

def list_container_contents(url, check_access=False, aggressive_check=False, download=False, download_dir=None,
                            include_files=None, include_dirs=None, exclude_files=None, exclude_dirs=None):
    """
    Fetch and parse all contents of a publicly accessible Azure Blob Container.
    Filters (include/exclude) apply to the list itself if not downloading, or to downloads if downloading.
    """
    parsed_input_url_container_name = get_container_name_from_url(url)
    current_container_name_for_logging = parsed_input_url_container_name
    extra = {'container_name': current_container_name_for_logging}

    try:
        all_contents = []
        marker = None
        page_number = 0
        total_objects_listed = 0 # Tracks objects that pass listing filters
        total_size_listed = 0    # Tracks size of objects that pass listing filters
        
        container_base_url = url.rstrip('/')

        while True:
            page_number += 1
            parsed_container_url = urllib.parse.urlparse(container_base_url)
            query_params = urllib.parse.parse_qs(parsed_container_url.query)
            query_params['restype'] = ['container']
            query_params['comp'] = ['list']
            
            if marker:
                query_params['marker'] = [marker]
            elif 'marker' in query_params:
                del query_params['marker']

            list_query_string = urllib.parse.urlencode(query_params, doseq=True, quote_via=urllib.parse.quote_plus)
            request_url = urllib.parse.urlunparse(parsed_container_url._replace(query=list_query_string))
            
            logger.info(f"Fetching page {page_number} (marker: {marker[:20] + '...' if marker and len(marker) > 20 else marker})...", extra=extra)
            response = requests.get(request_url, verify=False)
            response.raise_for_status()
            
            xml_text = response.text
            root = ET.fromstring(xml_text)

            if root.tag != 'EnumerationResults':
                raise ValueError("Unexpected XML format: root is not EnumerationResults")

            xml_container_full_url = root.get('ContainerName')
            if xml_container_full_url:
                actual_container_name = get_container_name_from_url(xml_container_full_url)
                if actual_container_name != "unknown_container":
                    current_container_name_for_logging = actual_container_name
                    extra['container_name'] = current_container_name_for_logging

            page_blobs_passing_list_filter = []
            blobs_element = root.find('Blobs')
            if blobs_element is not None:
                for blob_elem in blobs_element.findall('Blob'):
                    name_elem = blob_elem.find('Name')
                    properties_elem = blob_elem.find('Properties')
                    
                    if name_elem is None or properties_elem is None:
                        logger.warning("Skipping malformed Blob entry in XML", extra=extra)
                        continue

                    blob_name = name_elem.text
                    last_modified_str = properties_elem.find('Last-Modified').text
                    dt_obj = datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S GMT')
                    dt_obj_utc = dt_obj.replace(tzinfo=timezone.utc)
                    last_modified_iso = dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    size_str = properties_elem.find('Content-Length').text
                    etag_elem = properties_elem.find('Etag')
                    content_type_elem = properties_elem.find('Content-Type')
                    blob_type_elem = properties_elem.find('BlobType')

                    # --- Filtering logic for LISTING (if --download is NOT active) ---
                    if not download: # Apply filters to the list itself
                        should_be_listed = True
                        log_skip_reason = ""
                        if (exclude_files and matches_patterns(blob_name, exclude_files)) or \
                           (exclude_dirs and matches_patterns(blob_name, exclude_dirs, is_dir=True)):
                            should_be_listed = False
                            log_skip_reason = "matches exclude pattern for listing"
                        elif include_files or include_dirs:
                            if not (matches_patterns(blob_name, include_files) or \
                                    matches_patterns(blob_name, include_dirs, is_dir=True)):
                                should_be_listed = False
                                log_skip_reason = "does not match include pattern for listing"
                        
                        if not should_be_listed:
                            logger.debug(f"Skipping {blob_name} from listing: {log_skip_reason}", extra=extra)
                            continue # Skip this blob, don't add to list or count size/objects

                    # --- If we reach here, the item is eligible for listing (or download=True, so all are listed) ---
                    item = {
                        'Key': blob_name,
                        'LastModified': last_modified_iso,
                        'Size': int(size_str),
                        'ETag': etag_elem.text if etag_elem is not None else None,
                        'ContentType': content_type_elem.text if content_type_elem is not None else None,
                        'BlobType': blob_type_elem.text if blob_type_elem is not None else None
                    }
                    
                    is_accessible_for_check = True # Default for non-checked items
                    if check_access:
                        is_accessible_for_check, content_length_header = check_file_access(
                            container_base_url, blob_name, aggressive=aggressive_check, 
                            container_name=current_container_name_for_logging
                        )
                        item['is_accessible'] = is_accessible_for_check
                        item['content_length_from_header'] = content_length_header
                    
                    # --- Download Logic (applies if --download is active) ---
                    if download:
                        should_download_this_item = is_accessible_for_check if check_access else True
                        
                        # Apply download-specific filtering
                        if (exclude_files and matches_patterns(blob_name, exclude_files)) or \
                           (exclude_dirs and matches_patterns(blob_name, exclude_dirs, is_dir=True)):
                            logger.debug(f"Skipping download of {blob_name}: matches exclude pattern", extra=extra)
                            should_download_this_item = False
                        elif include_files or include_dirs:
                            if not (matches_patterns(blob_name, include_files) or \
                                    matches_patterns(blob_name, include_dirs, is_dir=True)):
                                logger.debug(f"Skipping download of {blob_name}: does not match include patterns", extra=extra)
                                should_download_this_item = False
                        
                        if should_download_this_item:
                            download_file(container_base_url, blob_name, download_dir, 
                                          current_container_name_for_logging)
                    
                    # Add to list of contents for JSON output and count towards totals
                    page_blobs_passing_list_filter.append(item)
                    total_size_listed += int(size_str) # Counts size of items that will be listed
            
            all_contents.extend(page_blobs_passing_list_filter)
            total_objects_listed += len(page_blobs_passing_list_filter) # Counts items that will be listed
            logger.info(f"Page {page_number}: {len(page_blobs_passing_list_filter)} blobs passed listing filters (Total listed so far: {total_objects_listed})", extra=extra)
            
            next_marker_elem = root.find('NextMarker')
            marker = next_marker_elem.text.strip() if next_marker_elem is not None and next_marker_elem.text and next_marker_elem.text.strip() else None
            
            if not marker:
                break
        
        logger.info(f"Finished fetching. Total blobs listed: {total_objects_listed}, Total size of listed blobs: {total_size_listed} bytes", extra=extra)
        
        timestamp = datetime.now(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return url, {
            'name': current_container_name_for_logging,
            'timestamp': timestamp,
            'total_items': total_objects_listed, # Reflects items passing list filters
            'total_size': total_size_listed,   # Reflects size of items passing list filters
            'contents': all_contents
        }
    
    except SSLError as e:
        return url, None
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error processing container: {str(e)}", extra=extra)
        return url, None
    except ET.ParseError as e:
        logger.error(f"XML parsing error for container: {str(e)}", extra=extra)
        logger.debug(f"Problematic XML content for {url}:\n{xml_text[:1000] if 'xml_text' in locals() else 'XML not available'}...", extra=extra)
        return url, None
    except Exception as e:
        logger.error(f"Error processing container {url}: {str(e)}", extra=extra)
        import traceback
        logger.debug(traceback.format_exc(), extra=extra)
        return url, None

def read_urls_from_file(file_path):
    """Read URLs from a text file."""
    try:
        with open(file_path, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return urls
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}", extra={'container_name': 'file_reader'})
        return []

def main():
    parser = argparse.ArgumentParser(
        description="List contents of Azure Blob Containers and output as JSON. Filters apply to the list or downloads.",
        epilog="Provide Azure Blob Container URLs directly or use --file. Include/Exclude filters apply to listing unless --download is used, then they apply to downloads.",
    )
    parser.add_argument('urls', nargs='*', help="Azure Blob Container URLs (e.g., https://youraccount.blob.core.windows.net/yourcontainer)")
    parser.add_argument('-f', '--file', help="Path to a text file containing Azure Blob Container URLs")
    parser.add_argument('--threads', type=int, default=1, help="Number of parallel threads for processing containers (default: 1)")
    
    access_group = parser.add_argument_group('Access Check Options')
    access_group.add_argument('--check-access', action='store_true', help="Perform requests to check blob accessibility (default: HEAD request)")
    access_group.add_argument('--aggressive', action='store_true', help="Use aggressive GET request with Range header for access checks (requires --check-access)")
    
    download_group = parser.add_argument_group('Download Options')
    download_group.add_argument('-d', '--download', action='store_true', help="Download accessible blobs. Filters will apply to downloads instead of listing.")
    download_group.add_argument('--download-dir', help="Specify a directory for downloaded blobs (defaults to a subfolder named after the container)")
    
    filter_group = parser.add_argument_group('Filtering Options (apply to listing OR download)')
    filter_group.add_argument('--include-file', action='append', default=[], help="Blob name patterns to include. Applies to listing, or to downloads if -d is used.")
    filter_group.add_argument('--include-dir', action='append', default=[], help="Blob path patterns to include. Applies to listing, or to downloads if -d is used.")
    filter_group.add_argument('--exclude-file', action='append', default=[], help="Blob name patterns to exclude. Applies to listing, or to downloads if -d is used.")
    filter_group.add_argument('--exclude-dir', action='append', default=[], help="Blob path patterns to exclude. Applies to listing, or to downloads if -d is used.")
    
    args = parser.parse_args()
    
    if args.aggressive and not args.check_access:
        logger.error("Error: --aggressive requires --check-access", extra={'container_name': 'main'})
        sys.exit(1)
    # REMOVED: The check that required --download for include/exclude filters.
    # if (args.include_file or args.include_dir or args.exclude_file or args.exclude_dir) and not args.download:
    #     logger.error("Error: --include-file, --include-dir, --exclude-file, and --exclude-dir require --download", extra={'container_name': 'main'})
    #     sys.exit(1)
    if args.threads < 1:
        logger.error("Error: --threads must be at least 1", extra={'container_name': 'main'})
        sys.exit(1)
    
    urls = args.urls
    if args.file:
        file_urls = read_urls_from_file(args.file)
        if not file_urls:
            logger.error("No valid URLs found in file. Exiting.", extra={'container_name': 'main'})
            sys.exit(1)
        urls.extend(file_urls)
    
    if not urls:
        logger.error("No URLs provided. Use URLs as arguments or --file option.", extra={'container_name': 'main'})
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    
    result_data = {}
    grand_total_items = 0
    grand_total_size = 0
    containers_processed = 0
    
    def process_container(url_item):
        return list_container_contents(
            url_item,
            check_access=args.check_access,
            aggressive_check=args.aggressive,
            download=args.download,
            download_dir=args.download_dir,
            include_files=args.include_file,
            include_dirs=args.include_dir,
            exclude_files=args.exclude_file,
            exclude_dirs=args.exclude_dir
        )
    
    logger.info(f"Starting processing of {len(urls)} container(s) with {args.threads} thread(s)...", extra={'container_name': 'main'})
    
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        processed_results = list(executor.map(process_container, urls))
    
    for url_key, container_data in processed_results:
        if container_data is not None:
            result_data[url_key] = container_data
            grand_total_items += container_data['total_items'] # These are now filtered totals
            grand_total_size += container_data['total_size']   # These are now filtered totals
            containers_processed += 1
    
    if containers_processed > 0: # Show summary if at least one container was processed
        summary_type = "listed" if not args.download else "found (downloads filtered separately)"
        logger.info(f"Final summary: Processed {containers_processed} containers. Total blobs {summary_type}: {grand_total_items}, Total size of {summary_type} blobs: {grand_total_size} bytes", extra={'container_name': 'main'})
    
    print(json.dumps(result_data, indent=2))
    
    if not result_data:
        logger.error("No containers were successfully processed or no items matched filters.", extra={'container_name': 'main'})
        #  sys.exit(1) # Commenting out exit(1) as empty result due to filters is not necessarily an error
        if containers_processed == 0 : # only exit if no containers were processed at all.
             sys.exit(1)


if __name__ == "__main__":
    main()