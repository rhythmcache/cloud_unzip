import zipfile
import fsspec
import os
import sys
import concurrent.futures
from typing import List, Optional, Union, BinaryIO

def format_size(size_in_bytes):
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_in_bytes)
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

def print_zip_tree(files, zipfile_obj):
    structure = {}

    for file_path in files:
        parts = file_path.split('/')
        current_level = structure
        for part in parts:
            current_level = current_level.setdefault(part, {})

    def print_nested(d, prefix="", path=""):
        keys = sorted(d.keys())
        for i, key in enumerate(keys):
            connector = "└── " if i == len(keys) - 1 else "├── "
            full_path = path + "/" + key if path else key
            if not d[key]:
                info = zipfile_obj.getinfo(full_path)
                size_str = format_size(info.file_size)
                print(f"{prefix}{connector}{key} ({size_str})")
            else:
                print(f"{prefix}{connector}{key}")
                
            print_nested(d[key], prefix + ("    " if connector == "└── " else "│   "), full_path)

    print_nested(structure)
    print(f"\nTotal files: {len(files)}")

class RemoteZipExtractor:
    def __init__(self, url: str):
        self.url = url
        self.fs = fsspec.filesystem("http")
        self.zipfile = None
        self._load_zipfile()
    
    def _load_zipfile(self):
        """Open the remote ZIP file using fsspec."""
        with self.fs.open(self.url) as remote_file:
            self.zipfile = zipfile.ZipFile(remote_file)
    
    def list_files(self) -> List[str]:
        """List all files in the ZIP archive."""
        return self.zipfile.namelist()
    
    def extract_file(self, filename: str, output_path: Optional[str] = None) -> str:
        """Extract a specific file from the ZIP archive."""
        if output_path is None:
            output_path = os.path.basename(filename)
        
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        with self.fs.open(self.url) as remote_file:
            with zipfile.ZipFile(remote_file) as zf:
                
                info = zf.getinfo(filename)
                total_size = info.file_size
                
                with zf.open(filename) as source, open(output_path, 'wb') as target:
                    chunk_size = 10 * 1024 * 1024
                    bytes_read = 0
                    while True:
                        chunk = source.read(chunk_size)
                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        target.write(chunk)
                        
                        progress = min(100, int(bytes_read * 100 / total_size))
                        readable_bytes = format_size(bytes_read)
                        readable_total = format_size(total_size)
                        print(f"\rExtracting '{filename}': {readable_bytes}/{readable_total} ({progress}%)", end="", file=sys.stderr)
                
                print(file=sys.stderr)
        
        return output_path
    
    def extract_files_parallel(self, filenames: List[str], output_dir: str, max_workers: int = None) -> List[str]:
        """Extract multiple files in parallel."""
        missing_files = [f for f in filenames if f not in self.zipfile.namelist()]
        if missing_files:
            raise ValueError(f"Files not found in the ZIP archive: {', '.join(missing_files)}")
        
        output_paths = []
        
        def extract_file_wrapper(filename: str) -> str:
            output_path = os.path.join(output_dir, filename)
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            print(f"Extracting '{filename}' to '{output_path}'...", file=sys.stderr)
            try:
                result = self.extract_file(filename, output_path)
                print(f"Successfully extracted '{filename}'", file=sys.stderr)
                return result
            except Exception as e:
                print(f"Failed to extract '{filename}': {str(e)}", file=sys.stderr)
                raise
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_filename = {executor.submit(extract_file_wrapper, filename): filename for filename in filenames}
            for future in concurrent.futures.as_completed(future_to_filename):
                filename = future_to_filename[future]
                try:
                    output_path = future.result()
                    output_paths.append(output_path)
                except Exception as e:
                    print(f"Error extracting '{filename}': {str(e)}", file=sys.stderr)
        
        return output_paths

def extract_file_from_remote_zip(url: str, filename: str, output_path: Optional[str] = None, to_stdout: bool = False) -> Union[str, bytes]:
    """Standalone function to extract a file from a remote ZIP archive."""
    fs = fsspec.filesystem("http")
    
    with fs.open(url) as remote_file:
        with zipfile.ZipFile(remote_file) as zf:
            if to_stdout:
                with zf.open(filename) as f:
                    return f.read()
            else:
                if output_path is None:
                    output_path = os.path.basename(filename)
                
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                
                info = zf.getinfo(filename)
                total_size = info.file_size
                
                with zf.open(filename) as source, open(output_path, 'wb') as target:
                    chunk_size = 10 * 1024 * 1024
                    bytes_read = 0
                    while True:
                        chunk = source.read(chunk_size)
                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        target.write(chunk)
                        
                        progress = min(100, int(bytes_read * 100 / total_size))
                        readable_bytes = format_size(bytes_read)
                        readable_total = format_size(total_size)
                        print(f"\rExtracting '{filename}': {readable_bytes}/{readable_total} ({progress}%)", end="", file=sys.stderr)
                
                print(file=sys.stderr)
                
                return output_path

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extract files from remote ZIP archives')
    parser.add_argument('url', help='URL of the remote ZIP file')
    parser.add_argument('-l', '--list', action='store_true', help='List files in the ZIP archive')
    parser.add_argument('-t', '--tree', action='store_true', help='Display zip contents in tree format')
    parser.add_argument('-e', '--extract', help='Extract specific files from the ZIP archive')
    parser.add_argument('-o', '--output', help='Output directory for extracted files. Use "-" to write to stdout')
    parser.add_argument('-p', '--parallel', action='store_true', help='Extract files in parallel')
    parser.add_argument('-w', '--workers', type=int, default=None, help='Maximum number of worker threads for parallel extraction')
    args = parser.parse_args()

    extractor = RemoteZipExtractor(args.url)

    if args.list or args.tree:
        files = extractor.list_files()
        if args.tree:
            print_zip_tree(files, extractor.zipfile)
        else:
            print(f"Files in the ZIP archive ({len(files)}):", file=sys.stderr)
            for file in files:
                file_info = extractor.zipfile.getinfo(file)
                size_str = format_size(file_info.file_size)
                print(f"  {file} ({size_str})", file=sys.stderr)

    if args.extract:
        files_to_extract = [f.strip() for f in args.extract.split(',')]
        
        if args.output == '-':
            if len(files_to_extract) > 1:
                print("Error: Cannot write multiple files to stdout", file=sys.stderr)
                sys.exit(1)
            file_data = extract_file_from_remote_zip(args.url, files_to_extract[0], to_stdout=True)
            sys.stdout.buffer.write(file_data)
        else:
            output_dir = args.output if args.output else '.'
            os.makedirs(output_dir, exist_ok=True)
            
            if args.parallel and len(files_to_extract) > 1:
                extractor.extract_files_parallel(files_to_extract, output_dir, max_workers=args.workers)
            else:
                for filename in files_to_extract:
                    output_path = os.path.join(output_dir, filename)
                    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                    print(f"Extracting '{filename}' to '{output_path}'...", file=sys.stderr)
                    extract_file_from_remote_zip(args.url, filename, output_path)
                    print(f"Successfully extracted '{filename}'", file=sys.stderr)

if __name__ == "__main__":
    main()
