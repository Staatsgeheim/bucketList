# bucketList: Cloud Storage Lister & Downloader Suite

A suite of Python utilities for listing and optionally downloading contents from publicly accessible cloud storage services:

* **AWS S3 Buckets** (`bucketListAWS.py`)
* **Azure Blob Storage Containers** (`bucketListAzure.py`)
* **Google Cloud Storage Buckets** (`bucketListGoogle.py`)

These tools are ideal for exploring public data sets, open cloud archives, or any other exposed cloud storage resources. All utilities output results in a structured JSON format suitable for analysis or further automation.

---

## Features

* **Multi-Cloud Support:** AWS S3, Azure Blob Storage, and Google Cloud Storage with a unified interface.
* **Content Listing:** Recursively lists the contents of public buckets/containers, with per-object metadata.
* **Filtering:**

  * Include or exclude items using wildcard patterns for file and directory names.
  * **Filtering logic:**

    * If **not downloading**, filters apply to items in the JSON output.
    * If **downloading**, all found items are listed (optionally with access checks); filters apply only to what gets downloaded.
* **Optional Downloading:** Download accessible files/blobs, preserving the original path structure.
* **Accessibility Checks:** Perform HEAD requests (or GET with `Range` for aggressive mode) to confirm access before download or for reporting.
* **Batch Processing:** Accepts multiple URLs directly or via a text file (with optional comments).
* **Concurrency:** Multi-threaded processing of multiple storage URLs for speed.
* **JSON Output:** Structured, detailed JSON output to standard output.
* **SSL/TLS Flexibility:** Ignores SSL/TLS verification warnings (helpful for some public endpoints; use with care).
* **Progress Logging:** Thread-safe logging, progress, and error reporting to standard error.

---

## Prerequisites

* Python 3.7 or higher
* `pip` (Python package installer)
* Python libraries:

  * `requests`
  * `pytz`

---

## Installation

### Option 1: Using `pip` & Virtualenv (Linux/macOS)

1. **Clone this repository:**

   ```bash
   git clone https://github.com/Staatsgeheim/bucketList.git
   cd bucketList
   ```

2. **Create and activate a virtual environment (recommended):**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**

   ```bash
   pip install requests pytz
   ```

4. **(Optional) Make scripts executable:**

   ```bash
   chmod +x bucketListAWS.py bucketListAzure.py bucketListGoogle.py
   ```

### Option 2: Using Conda (Cross-platform)

1. **Create and activate a conda environment:**

   ```bash
   conda create -n bucketlist python=3.9
   conda activate bucketlist
   ```

2. **Install dependencies via conda-forge:**

   ```bash
   conda install -c conda-forge requests pytz
   ```

   *(add `-y` for non-interactive installs: `conda install -y -c conda-forge requests pytz`)*

   *If you get a package not found error, ensure you have conda-forge enabled or fall back to:*

   ```bash
   pip install requests pytz
   ```

### Option 3: Windows Installation

1. **Clone the repository and create a virtual environment:**

   ```powershell
   git clone https://github.com/Staatsgeheim/bucketList.git
   cd bucketList
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. **Install dependencies:**

   ```powershell
   pip install requests pytz
   ```

---

## Usage

All scripts share a similar command-line interface.

**General command structure:**

```bash
python script_name.py [URLs...] [options]
```

Where:

* `script_name.py` is one of the three tools (`bucketListAWS.py`, `bucketListAzure.py`, `bucketListGoogle.py`).
* `[URLs...]` are one or more storage URLs (can also use `-f FILE` for a list).
* `[options]` are command-line flags (see below).

**Key Options:**

| Option                   | Description                                                      |
| ------------------------ | ---------------------------------------------------------------- |
| URLs (positional)        | One or more URLs to public buckets/containers                    |
| `-f, --file FILE`        | File with one URL per line (lines starting with `#` are ignored) |
| `--threads N`            | Number of parallel threads (default: 1)                          |
| `--check-access`         | HEAD request to check item accessibility, report in JSON         |
| `--aggressive`           | Use GET/Range for access check (requires `--check-access`)       |
| `-d, --download`         | Download accessible items matching filters                       |
| `--download-dir DIR`     | Directory for downloads (default: name of bucket/container)      |
| `--include-file PATTERN` | Wildcard pattern for file names to include (can repeat)          |
| `--include-dir PATTERN`  | Wildcard pattern for directories to include (can repeat)         |
| `--exclude-file PATTERN` | Wildcard pattern for file names to exclude (can repeat)          |
| `--exclude-dir PATTERN`  | Wildcard pattern for directories to exclude (can repeat)         |

**Filtering logic:**

* If `--download` is **not** used, filters determine what is output in JSON.
* If `--download` **is** used, filters determine what is downloaded (all items are listed in JSON (Yes I know how stupid this is, will fix it later) access checks shown if enabled).

---

## Output Format

Output is a JSON object keyed by storage URL, each containing metadata and a list of objects/blobs:

```json
{
  "https://storage-url1/...": {
    "name": "bucket-or-container-name-1",
    "timestamp": "YYYY-MM-DDTHH:MM:SSZ",
    "total_items": 123,
    "total_size": 123456789,
    "contents": [
      {
        "Key": "object_or_blob_name_with_path.ext",
        "LastModified": "YYYY-MM-DDTHH:MM:SSZ",
        "Size": 1024,
        "ETag": "md5hash",
        "StorageClass": "STANDARD",
        "Owner": { "ID": "owner-id", "DisplayName": "owner-name" },
        "ContentType": "application/octet-stream",
        "BlobType": "BlockBlob",
        "is_accessible": true,
        "content_length_from_header": "1024"
      }
    ]
  },
  ...
}
```

Fields like `Owner`, `ContentType`, `BlobType`, `is_accessible`, `content_length_from_header` are included only if available for that provider or access check.

---

## Provider-Specific Usage & Examples

### AWS S3 (`bucketListAWS.py`)

List contents:

```bash
python bucketListAWS.py https://s3.amazonaws.com/commoncrawl/ --check-access
```

List only `.txt` files in a folder:

```bash
python bucketListAWS.py https://s3.amazonaws.com/my-public-bucket/ --include-dir "documents/" --include-file "*.txt"
```

Download from multiple buckets in parallel:

```bash
python bucketListAWS.py -f urls_aws.txt -d --download-dir ./s3_downloads --threads 4
```

### Azure Blob (`bucketListAzure.py`)

List blobs:

```bash
python bucketListAzure.py https://myopendata.blob.core.windows.net/data --check-access
```

Download all `.zip` files, excluding a `temp/` directory:

```bash
python bucketListAzure.py https://myaccount.blob.core.windows.net/mycontainer -d --include-file "*.zip" --exclude-dir "temp/"
```

### Google Cloud Storage (`bucketListGoogle.py`)

List GCS bucket contents:

```bash
python bucketListGoogle.py https://storage.googleapis.com/gcp-public-data-landsat --threads 2
```

Download files by prefix:

```bash
python bucketListGoogle.py https://storage.googleapis.com/my-gcs-data/ -d --include-dir "processed_images/"
```

---

## License

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

---

## Disclaimer

These tools are for lawful research and analysis of data you are authorized to access. Do not use on storage endpoints you lack permission for. Always comply with laws and terms of service.

---

