# cloud_unzip
python script to extract files from remote ZIP archives without downloading the entire archive


## Installation

```
pip install git+https://github.com/rhythmcache/cloud_unzip.git
```


### Usage

```
usage: cloud_unzip [-h] [-l] [-t] [-e EXTRACT] [-o OUTPUT] [-p] [-w WORKERS] url

Extract files from remote ZIP archives

positional arguments:
  url                   URL of the remote ZIP file

options:
  -h, --help                 show this help message and exit
  -l, --list                 List files in the ZIP archive
  -t, --tree                 Display zip contents in tree format
  -e, --extract EXTRACT      Extract specific files from the ZIP archive (comma-separated)
  -o, --output OUTPUT        Output directory for extracted files. Use "-" to write to stdout
  -p, --parallel             Extract files in parallel
  -w, --workers <n>          Maximum number of worker threads for parallel extraction
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
( use  `--parallel` to extract multiple files parallely , default extraction method is sequential )


### Limitations 
- Server must support range request
- only `Deflate` and `Store` methods are currently supported 
