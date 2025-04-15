import httpx
import zipfile
import io
import os
import struct
import zlib
import concurrent.futures
import sys
from typing import BinaryIO, Dict, List, Optional, Tuple, Union, Callable


def print_zip_tree(files):
    structure = {}

    for file_path in files:
        parts = file_path.split('/')
        current_level = structure
        for part in parts:
            current_level = current_level.setdefault(part, {})

    def print_nested(d, prefix=""):
        keys = sorted(d.keys())
        for i, key in enumerate(keys):
            connector = "└── " if i == len(keys) - 1 else "├── "
            print(prefix + connector + key)
            print_nested(d[key], prefix + ("    " if connector == "└── " else "│   "))

    print_nested(structure)
    print(f"\nTotal files: {len(files)}")

class RemoteZipExtractor:
    def __init__(self, url: str):
        self.url = url
        self.client = httpx.Client()
        self.file_size = self._get_file_size()
        self.central_directory = None
        self.files_info = None
        self.is_zip64 = False

    def _get_file_size(self) -> int:
       response = httpx.head(self.url, follow_redirects=True)
       response.raise_for_status()
       return int(response.headers.get('Content-Length', 0))

    def _get_range(self, start: int, end: int) -> bytes:
       headers = {'Range': f'bytes={start}-{end}'}
       response = httpx.get(self.url, headers=headers, follow_redirects=True)
       response.raise_for_status()
       return response.content


    def _find_end_of_central_directory(self) -> Tuple[int, bytes, Optional[bytes]]:
        chunk_size = 1024
        max_comment_size = 65535
        start_pos = max(0, self.file_size - chunk_size - max_comment_size - 22)
        end_pos = self.file_size - 1
        data = self._get_range(start_pos, end_pos)
        pos = data.rfind(b'\x50\x4b\x05\x06')
        if pos == -1:
            raise ValueError("Could not find End of Central Directory record")
        eocd_offset = start_pos + pos
        eocd_record = data[pos:pos+22]
        z64_locator_pos = pos - 20
        zip64_eocd_record = None
        if z64_locator_pos >= 0 and data[z64_locator_pos:z64_locator_pos+4] == b'\x50\x4b\x06\x07':
            self.is_zip64 = True
            zip64_eocd_offset = struct.unpack('<Q', data[z64_locator_pos+8:z64_locator_pos+16])[0]
            zip64_header = self._get_range(zip64_eocd_offset, zip64_eocd_offset + 12 - 1)
            if zip64_header[:4] != b'\x50\x4b\x06\x06':
                raise ValueError("Invalid ZIP64 End of Central Directory Record signature")
            record_size = struct.unpack('<Q', zip64_header[4:12])[0]
            zip64_eocd_record = self._get_range(zip64_eocd_offset, zip64_eocd_offset + record_size + 12 - 1)
        return eocd_offset, eocd_record, zip64_eocd_record

    def _parse_zip64_extra_field(self, extra_field: bytes, file_info: Dict) -> Dict:
        pos = 0
        while pos + 4 <= len(extra_field):
            header_id = struct.unpack('<H', extra_field[pos:pos+2])[0]
            data_size = struct.unpack('<H', extra_field[pos+2:pos+4])[0]
            if header_id == 0x0001:
                data = extra_field[pos+4:pos+4+data_size]
                offset = 0
                if file_info['uncompressed_size'] == 0xFFFFFFFF and offset + 8 <= data_size:
                    file_info['uncompressed_size'] = struct.unpack('<Q', data[offset:offset+8])[0]
                    offset += 8
                if file_info['compressed_size'] == 0xFFFFFFFF and offset + 8 <= data_size:
                    file_info['compressed_size'] = struct.unpack('<Q', data[offset:offset+8])[0]
                    offset += 8
                if file_info['local_header_offset'] == 0xFFFFFFFF and offset + 8 <= data_size:
                    file_info['local_header_offset'] = struct.unpack('<Q', data[offset:offset+8])[0]
                    offset += 8
            pos += 4 + data_size
        return file_info

    def _read_central_directory(self) -> Dict[str, Dict]:
        eocd_offset, eocd_data, zip64_eocd_record = self._find_end_of_central_directory()
        if self.is_zip64 and zip64_eocd_record:
            cd_offset = struct.unpack('<Q', zip64_eocd_record[48:56])[0]
            cd_size = struct.unpack('<Q', zip64_eocd_record[40:48])[0]
            total_entries = struct.unpack('<Q', zip64_eocd_record[32:40])[0]
        else:
            cd_offset = struct.unpack('<I', eocd_data[16:20])[0]
            cd_size = struct.unpack('<I', eocd_data[12:16])[0]
            total_entries = struct.unpack('<H', eocd_data[10:12])[0]
            if cd_offset == 0xFFFFFFFF or cd_size == 0xFFFFFFFF or total_entries == 0xFFFF:
                if not self.is_zip64 or not zip64_eocd_record:
                    raise ValueError("ZIP64 values indicated but no ZIP64 End of Central Directory Record found")
                cd_offset = struct.unpack('<Q', zip64_eocd_record[48:56])[0]
                cd_size = struct.unpack('<Q', zip64_eocd_record[40:48])[0]
                total_entries = struct.unpack('<Q', zip64_eocd_record[32:40])[0]
        cd_data = self._get_range(cd_offset, cd_offset + cd_size - 1)
        self.central_directory = cd_data
        files_info = {}
        pos = 0
        while pos < len(cd_data):
            if cd_data[pos:pos+4] != b'\x50\x4b\x01\x02':
                break
            file_header = cd_data[pos:pos+46]
            pos += 46
            name_length = struct.unpack('<H', file_header[28:30])[0]
            extra_field_length = struct.unpack('<H', file_header[30:32])[0]
            comment_length = struct.unpack('<H', file_header[32:34])[0]
            compression_method = struct.unpack('<H', file_header[10:12])[0]
            compressed_size = struct.unpack('<I', file_header[20:24])[0]
            uncompressed_size = struct.unpack('<I', file_header[24:28])[0]
            local_header_offset = struct.unpack('<I', file_header[42:46])[0]
            filename = cd_data[pos:pos+name_length].decode('utf-8', errors='replace')
            pos += name_length
            extra_field = cd_data[pos:pos+extra_field_length]
            pos += extra_field_length
            pos += comment_length
            file_info = {
                'compression_method': compression_method,
                'compressed_size': compressed_size,
                'uncompressed_size': uncompressed_size,
                'local_header_offset': local_header_offset
            }
            if compressed_size == 0xFFFFFFFF or uncompressed_size == 0xFFFFFFFF or local_header_offset == 0xFFFFFFFF:
                file_info = self._parse_zip64_extra_field(extra_field, file_info)
            files_info[filename] = file_info
        self.files_info = files_info
        return files_info

    def list_files(self) -> List[str]:
        if self.files_info is None:
            self._read_central_directory()
        return list(self.files_info.keys())

    def extract_file(self, filename: str, output_path: Optional[str] = None) -> str:
        if self.files_info is None:
            self._read_central_directory()
        if filename not in self.files_info:
            raise ValueError(f"File '{filename}' not found in the ZIP archive")
        file_info = self.files_info[filename]
        local_header_offset = file_info['local_header_offset']
        local_header = self._get_range(local_header_offset, local_header_offset + 30 - 1)
        if local_header[:4] != b'\x50\x4b\x03\x04':
            raise ValueError(f"Invalid local file header for '{filename}'")
        name_length = struct.unpack('<H', local_header[26:28])[0]
        extra_field_length = struct.unpack('<H', local_header[28:30])[0]
        data_offset = local_header_offset + 30 + name_length + extra_field_length
        compressed_size = file_info['compressed_size']
        compression_method = file_info['compression_method']
        if compressed_size > 50 * 1024 * 1024:
            with open(output_path or os.path.basename(filename), 'wb') as f:
                chunk_size = 10 * 1024 * 1024
                for chunk_start in range(0, compressed_size, chunk_size):
                    chunk_end = min(chunk_start + chunk_size - 1, data_offset + compressed_size - 1)
                    chunk = self._get_range(data_offset + chunk_start, chunk_end)
                    is_first_chunk = chunk_start == 0
                    is_last_chunk = chunk_end >= data_offset + compressed_size - 1
                    if compression_method == 0:
                        f.write(chunk)
                    elif compression_method == 8:
                        if is_first_chunk and is_last_chunk:
                            decompressor = zlib.decompressobj(-15)
                            f.write(decompressor.decompress(chunk))
                            f.write(decompressor.flush())
                        else:
                            compressed_data = self._get_range(data_offset, data_offset + compressed_size - 1)
                            decompressor = zlib.decompressobj(-15)
                            f.write(decompressor.decompress(compressed_data))
                            f.write(decompressor.flush())
                            break
            return output_path or os.path.basename(filename)
        compressed_data = self._get_range(data_offset, data_offset + compressed_size - 1)
        if compression_method == 0:
            file_data = compressed_data
        elif compression_method == 8:
            decompressor = zlib.decompressobj(-15)
            file_data = decompressor.decompress(compressed_data)
            file_data += decompressor.flush()
        else:
            raise ValueError(f"Unsupported compression method: {compression_method}")
        if output_path is None:
            output_path = os.path.basename(filename)
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(file_data)
        return output_path
    
    def extract_files_parallel(self, filenames: List[str], output_dir: str, max_workers: int = None) -> List[str]:
        if self.files_info is None:
            self._read_central_directory()
            
        missing_files = [f for f in filenames if f not in self.files_info]
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
    extractor = RemoteZipExtractor(url)
    if to_stdout:
        file_info = extractor.files_info[filename] if extractor.files_info else extractor._read_central_directory()[filename]
        local_header_offset = file_info['local_header_offset']
        local_header = extractor._get_range(local_header_offset, local_header_offset + 30 - 1)
        name_length = struct.unpack('<H', local_header[26:28])[0]
        extra_field_length = struct.unpack('<H', local_header[28:30])[0]
        data_offset = local_header_offset + 30 + name_length + extra_field_length
        compressed_size = file_info['compressed_size']
        compression_method = file_info['compression_method']
        compressed_data = extractor._get_range(data_offset, data_offset + compressed_size - 1)
        if compression_method == 0:
            return compressed_data
        elif compression_method == 8:
            decompressor = zlib.decompressobj(-15)
            file_data = decompressor.decompress(compressed_data)
            file_data += decompressor.flush()
            return file_data
        else:
            raise ValueError(f"Unsupported compression method: {compression_method}")
    return extractor.extract_file(filename, output_path)

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
            print_zip_tree(files)
        else:
            print(f"Files in the ZIP archive ({len(files)}):", file=sys.stderr)
            for file in files:
                file_info = extractor.files_info[file]
                print(f"  {file} ({file_info['uncompressed_size']} bytes)", file=sys.stderr)
    
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
