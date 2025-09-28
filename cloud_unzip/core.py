import zipfile
import fsspec
import os
import sys
import concurrent.futures
import getpass
import fnmatch
import re
from typing import List, Optional, Union, BinaryIO, Pattern

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

def match_files(file_list: List[str], patterns: List[str], use_regex: bool = False) -> List[str]:
    """
    Match files using glob patterns or regex.
    
    Args:
        file_list: List of file paths to match against
        patterns: List of patterns to match
        use_regex: If True, treat patterns as regex; otherwise use glob patterns
    
    Returns:
        List of matching file paths
    """
    matched_files = set()
    
    for pattern in patterns:
        if use_regex:
            try:
                regex_pattern = re.compile(pattern)
                for file_path in file_list:
                    if regex_pattern.search(file_path):
                        matched_files.add(file_path)
            except re.error as e:
                print(f"Invalid regex pattern '{pattern}': {e}", file=sys.stderr)
                continue
        else:
            # Use glob pattern matching
            for file_path in file_list:
                if fnmatch.fnmatch(file_path, pattern):
                    matched_files.add(file_path)
    
    return sorted(list(matched_files))

def print_zip_tree(files, zipfile_obj):
    structure = {}
    folder_count = 0

    for file_path in files:
        parts = file_path.split('/')
        current_level = structure
        
        for i, part in enumerate(parts):
            if i < len(parts) - 1:
                if part not in current_level:
                    folder_count += 1
            current_level = current_level.setdefault(part, {})

    def print_nested(d, prefix="", path=""):
        keys = sorted(d.keys())
        for i, key in enumerate(keys):
            connector = "└── " if i == len(keys) - 1 else "├── "
            full_path = path + "/" + key if path else key
            
            print(f"{prefix}{connector}{key}")
            
            print_nested(d[key], prefix + ("    " if connector == "└── " else "│   "), full_path)

    print_nested(structure)
    print(f"\nTotal files: {len(files)}")
    print(f"Total folders: {folder_count}")

def get_password(provided_password=None):
    """Get password from argument or prompt user if needed"""
    if provided_password:
        return provided_password
    return getpass.getpass("Enter ZIP password: ")

class RemoteZipExtractor:
    def __init__(self, url: str, password: Optional[str] = None):
        self.url = url
        self.fs = fsspec.filesystem("http")
        self.zipfile = None
        self.password = password
        self._load_zipfile()
    
    def _load_zipfile(self):
        """Open the remote ZIP file using fsspec."""
        try:
            with self.fs.open(self.url) as remote_file:
                try:
                    self.zipfile = zipfile.ZipFile(remote_file)
                    self._check_if_encrypted()
                except (zipfile.BadZipFile, RuntimeError) as e:
                    if "encrypted" in str(e).lower() or "password required" in str(e).lower():
                        self._reopen_with_password()
                    else:
                        raise
        except Exception as e:
            print(f"Error opening ZIP file: {str(e)}", file=sys.stderr)
            raise
    
    def _check_if_encrypted(self):
        """Check if the ZIP file is encrypted and get password if needed."""
        try:
            test_file = self.zipfile.namelist()[0]
            self.zipfile.open(test_file)
        except RuntimeError as e:
            if "password required" in str(e).lower() or "encrypted" in str(e).lower():
                self._reopen_with_password()
    
    def _reopen_with_password(self):
        """Reopen the ZIP file with a password."""
        if not self.password:
            self.password = get_password()
        
        with self.fs.open(self.url) as remote_file:
            try:
                self.zipfile = zipfile.ZipFile(remote_file)
                test_file = self.zipfile.namelist()[0]
                self.zipfile.open(test_file, pwd=self.password.encode('utf-8') if self.password else None)
            except (zipfile.BadZipFile, RuntimeError) as e:
                if "incorrect password" in str(e).lower() or "bad password" in str(e).lower():
                    print("Incorrect password. Please try again.", file=sys.stderr)
                    self.password = get_password()
                    self._reopen_with_password()
                else:
                    raise
    
    def list_files(self) -> List[str]:
        """List all files in the ZIP archive."""
        return self.zipfile.namelist()
    
    def find_files(self, patterns: List[str], use_regex: bool = False) -> List[str]:
        """Find files matching the given patterns."""
        all_files = self.list_files()
        return match_files(all_files, patterns, use_regex)
    
    def extract_file(self, filename: str, output_path: Optional[str] = None, flatten: bool = False) -> str:
        """Extract a specific file from the ZIP archive."""
        if output_path is None:
            if flatten:
                output_path = os.path.basename(filename)
            else:
                output_path = filename
        
        # only create directories if we're not flattening or if output_path has directories
        if not flatten or os.path.dirname(output_path):
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        with self.fs.open(self.url) as remote_file:
            with zipfile.ZipFile(remote_file) as zf:
                
                info = zf.getinfo(filename)
                total_size = info.file_size
                
                try:
                    with zf.open(filename, pwd=self.password.encode('utf-8') if self.password else None) as source, open(output_path, 'wb') as target:
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
                except RuntimeError as e:
                    if "password required" in str(e).lower() or "bad password" in str(e).lower():
                        print(f"\nThe file '{filename}' requires a password.", file=sys.stderr)
                        self.password = get_password(self.password)
                        return self.extract_file(filename, output_path)
                    else:
                        raise
        
        return output_path
    
    def extract_files_parallel(self, filenames: List[str], output_dir: str, max_workers: int = None, flatten: bool = False) -> List[str]:
        """Extract multiple files in parallel."""
        missing_files = [f for f in filenames if f not in self.zipfile.namelist()]
        if missing_files:
            raise ValueError(f"Files not found in the ZIP archive: {', '.join(missing_files)}")
        
        output_paths = []
        
        def extract_file_wrapper(filename: str) -> str:
            if flatten:
                output_path = os.path.join(output_dir, os.path.basename(filename))
            else:
                output_path = os.path.join(output_dir, filename)
            
            # only create directories if we're not flattening or if there are still directories in the path
            if not flatten or os.path.dirname(os.path.relpath(output_path, output_dir)):
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            
            print(f"Extracting '{filename}' to '{output_path}'...", file=sys.stderr)
            try:
                result = self.extract_file(filename, output_path, flatten=False)  # don't double-flatten
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

def extract_file_from_remote_zip(url: str, filename: str, output_path: Optional[str] = None, to_stdout: bool = False, password: Optional[str] = None, flatten: bool = False) -> Union[str, bytes]:
    """Standalone function to extract a file from a remote ZIP archive."""
    fs = fsspec.filesystem("http")
    pwd_bytes = password.encode('utf-8') if password else None
    
    with fs.open(url) as remote_file:
        with zipfile.ZipFile(remote_file) as zf:
            try:
                if to_stdout:
                    with zf.open(filename, pwd=pwd_bytes) as f:
                        return f.read()
                else:
                    if output_path is None:
                        if flatten:
                            output_path = os.path.basename(filename)
                        else:
                            output_path = filename
                    
                    # only create directories if we're not flattening or if output_path has directories
                    if not flatten or os.path.dirname(output_path):
                        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                    
                    info = zf.getinfo(filename)
                    total_size = info.file_size
                    
                    with zf.open(filename, pwd=pwd_bytes) as source, open(output_path, 'wb') as target:
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
            except RuntimeError as e:
                if "password required" in str(e).lower() or "bad password" in str(e).lower():
                    actual_password = get_password(password)
                    return extract_file_from_remote_zip(url, filename, output_path, to_stdout, actual_password, flatten)
                else:
                    raise

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extract files from remote ZIP archives')
    parser.add_argument('url', help='URL of the remote ZIP file')
    parser.add_argument('-l', '--list', action='store_true', help='List files in the ZIP archive')
    parser.add_argument('-t', '--tree', action='store_true', help='Display zip contents in tree format')
    parser.add_argument('-e', '--extract', help='Extract specific files from the ZIP archive (supports glob patterns)')
    parser.add_argument('-f', '--find', help='Find files matching patterns (supports glob patterns)')
    parser.add_argument('-r', '--regex', action='store_true', help='Use regex patterns instead of glob patterns')
    parser.add_argument('-o', '--output', help='Output directory for extracted files. Use "-" to write to stdout')
    parser.add_argument('-p', '--parallel', action='store_true', help='Extract files in parallel')
    parser.add_argument('-w', '--workers', type=int, default=None, help='Maximum number of worker threads for parallel extraction')
    parser.add_argument('--flatten', action='store_true', help='Extract files without preserving directory structure')
    parser.add_argument('--password', help='Password for encrypted ZIP files')
    args = parser.parse_args()

    extractor = RemoteZipExtractor(args.url, password=args.password)

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

    if args.find:
        patterns = [p.strip() for p in args.find.split(',')]
        matching_files = extractor.find_files(patterns, use_regex=args.regex)
        
        pattern_type = "regex" if args.regex else "glob"
        print(f"Files matching {pattern_type} patterns ({len(matching_files)}):", file=sys.stderr)
        for file in matching_files:
            file_info = extractor.zipfile.getinfo(file)
            size_str = format_size(file_info.file_size)
            print(f"  {file} ({size_str})", file=sys.stderr)

    if args.extract:
        # check if we're dealing with patterns or explicit file names
        input_patterns = [p.strip() for p in args.extract.split(',')]
        
        # if any pattern contains wildcards or we're in regex mode, treat as patterns
        if args.regex or any('*' in p or '?' in p or '[' in p for p in input_patterns):
            files_to_extract = extractor.find_files(input_patterns, use_regex=args.regex)
            if not files_to_extract:
                pattern_type = "regex" if args.regex else "glob"
                print(f"No files found matching {pattern_type} patterns: {', '.join(input_patterns)}", file=sys.stderr)
                sys.exit(1)
            
            pattern_type = "regex" if args.regex else "glob"
            print(f"Found {len(files_to_extract)} files matching {pattern_type} patterns", file=sys.stderr)
        else:
            files_to_extract = input_patterns
        
        if args.output == '-':
            if len(files_to_extract) > 1:
                print("Error: Cannot write multiple files to stdout", file=sys.stderr)
                sys.exit(1)
            file_data = extract_file_from_remote_zip(args.url, files_to_extract[0], to_stdout=True, password=args.password)
            sys.stdout.buffer.write(file_data)
        else:
            output_dir = args.output if args.output else '.'
            os.makedirs(output_dir, exist_ok=True)
            
            if args.parallel and len(files_to_extract) > 1:
                extractor.extract_files_parallel(files_to_extract, output_dir, max_workers=args.workers, flatten=args.flatten)
            else:
                for filename in files_to_extract:
                    if args.flatten:
                        output_path = os.path.join(output_dir, os.path.basename(filename))
                    else:
                        output_path = os.path.join(output_dir, filename)
                    
                    # handle filename conflicts when flattening
                    if args.flatten and os.path.exists(output_path):
                        base, ext = os.path.splitext(os.path.basename(filename))
                        counter = 1
                        while os.path.exists(output_path):
                            output_path = os.path.join(output_dir, f"{base}_{counter}{ext}")
                            counter += 1
                        print(f"Warning: File name conflict, renaming to '{os.path.basename(output_path)}'", file=sys.stderr)
                    
                    # only create directories if we're not flattening or if there are still directories in the path
                    if not args.flatten or os.path.dirname(os.path.relpath(output_path, output_dir)):
                        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                    
                    print(f"Extracting '{filename}' to '{output_path}'...", file=sys.stderr)
                    extract_file_from_remote_zip(args.url, filename, output_path, password=args.password, flatten=args.flatten)
                    print(f"Successfully extracted '{filename}'", file=sys.stderr)

if __name__ == "__main__":
    main()