# cloud_unzip
python script to extract files from remote ZIP archives without downloading the entire archive



## Installation

```
pip install cloud_unzip
```


### Usage

```
usage: cloud_unzip [-h] [-l] [-t] [-e EXTRACT] [-f FIND] [-r] [-o OUTPUT] [-p] [-w WORKERS] [--flatten] [--password PASSWORD] url

Extract files from remote ZIP archives

positional arguments:
  url                   URL of the remote ZIP file

options:
  -h, --help            show this help message and exit
  -l, --list            List files in the ZIP archive
  -t, --tree            Display zip contents in tree format
  -e EXTRACT, --extract EXTRACT
                        Extract specific files from the ZIP archive (supports glob patterns)
  -f FIND, --find FIND  Find files matching patterns (supports glob patterns)
  -r, --regex           Use regex patterns instead of glob patterns
  -o OUTPUT, --output OUTPUT
                        Output directory for extracted files. Use "-" to write to stdout
  -p, --parallel        Extract files in parallel
  -w WORKERS, --workers WORKERS
                        Maximum number of worker threads for parallel extraction
  --flatten             Extract files without preserving directory structure
  --password PASSWORD   Password for encrypted ZIP files
```

#### To extract a single file

```
cloud_unzip -e path/to/file/inside/zip <url>
```
#### To extract Multiple files
- Enter file paths comma separated 
```
cloud_unzip -e path/to/file1,path/to/file2,path/to/file3 <url>
```
---


-  use  `--parallel` to extract multiple files parallely , default extraction method is sequential 

- If the ZIP file is `encrypted`, it will ask for a `password` during extraction, or it can be provided using the `--password <your password>` argument.


---

### Limitations 
- Server must support range request
<!--
- only `Deflate` and `Store` methods are currently supported
-->



### Use as module

- Example

```python
from cloud_unzip import RemoteZipExtractor

url = "https://example.com/yourfile.zip"
extractor = RemoteZipExtractor(url)

# List files in the ZIP archive
files = extractor.list_files()
print("Files in zip:", files)

# Extract a specific file
extracted_path = extractor.extract_file("docs/readme.txt", output_path="readme.txt")
print(f"Extracted to: {extracted_path}")
```
