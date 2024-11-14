import os
from io import BytesIO
from zipfile import ZipFile, BadZipFile
import requests
from tqdm import tqdm
import csv
from concurrent.futures import ThreadPoolExecutor


class ZipInspector:
    def __init__(self, url):
        """
        Initialize the ZipInspector with a URL.
        Args:
            url (str): URL of the ZIP file to inspect.
        """
        self.url = url
        self.file_sizes = {}
        self.file_list = []
        self.content_length = 0

    def fetch_bytes(self, byte_range):
        """
        Fetch a specific range of bytes from the file with progress display.
        Args:
            byte_range (str): Range of bytes to fetch (e.g., "0-1048575").
        Returns:
            bytes: The requested portion of the file.
        """
        headers = {"Range": f"bytes={byte_range}"}
        response = requests.get(self.url, headers=headers, stream=True)
        if response.status_code in (206, 200):  # Partial Content or OK
            total_size = int(response.headers.get("Content-Length", len(response.content)))
            chunk_size = 1024  # 1 KB chunks
            content = BytesIO()

            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {byte_range}") as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        content.write(chunk)
                        pbar.update(len(chunk))

            return content.getvalue()
        else:
            raise Exception(f"Failed to fetch byte range {byte_range}. HTTP Status: {response.status_code}")

    def locate_central_directory(self, step=1048576, max_attempts=20):
        """
        Locate the central directory of the ZIP file.
        Args:
            step (int): Number of bytes to fetch in each attempt.
            max_attempts (int): Maximum number of attempts to locate the directory.
        Returns:
            bytes: The central directory bytes.
        """
        for attempt in range(max_attempts):
            start = max(self.content_length - step * (attempt + 1), 0)
            end = self.content_length - 1
            print(f"Fetching bytes {start}-{end} (Attempt {attempt + 1})...")
            try:
                central_dir_bytes = self.fetch_bytes(f"{start}-{end}")
                with ZipFile(BytesIO(central_dir_bytes)) as zf:
                    print("Central directory located successfully.")
                    return central_dir_bytes
            except BadZipFile:
                print(f"Attempt {attempt + 1} failed. Retrying with larger range.")
            except Exception as e:
                print(f"Unexpected error: {e}")

        raise Exception("Failed to locate central directory after multiple attempts.")

    def inspect(self):
        """
        Inspect the structure of the ZIP file without downloading the entire file.
        """
        print("Fetching metadata to get the file size...")
        response = requests.head(self.url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch file metadata. HTTP Status: {response.status_code}")

        self.content_length = int(response.headers["Content-Length"])
        print(f"File size: {self.content_length} bytes")

        print("Fetching initial bytes to detect ZIP structure...")
        initial_bytes = self.fetch_bytes("0-8191")
        if not initial_bytes.startswith(b"PK"):
            raise Exception("This does not appear to be a valid ZIP file.")

        print("Valid ZIP detected. Attempting to locate the central directory...")
        central_dir_bytes = self.locate_central_directory()

        zip_data = initial_bytes + central_dir_bytes

        try:
            with ZipFile(BytesIO(zip_data)) as zf:
                self.file_list = zf.namelist()
                self.file_sizes = {name: zf.getinfo(name).file_size for name in self.file_list}
        except Exception as e:
            raise Exception(f"Failed to process ZIP structure: {e}")

    def navigate_and_display(self):
        """
        Navigate through folders and display files with sizes interactively.
        """
        folder_structure = {}

        # Build folder structure
        for file_name in self.file_list:
            parts = file_name.split("/")
            current_level = folder_structure
            for part in parts[:-1]:
                current_level = current_level.setdefault(part, {})
            current_level[parts[-1]] = self.file_sizes.get(file_name, 0)

        def display_structure(structure, path=""):
            while True:
                print("\nCurrent Folder:", path or "/")
                items = list(structure.keys())
                for idx, item in enumerate(items, start=1):
                    if isinstance(structure[item], dict):
                        print(f"[{idx}] {item}/")
                    else:
                        print(f"[{idx}] {item} ({structure[item]} bytes)")

                print("\nOptions:")
                print("Enter folder number to navigate into it.")
                print("Enter 'b' to go back.")
                print("Enter 'r' to generate a report (text or CSV).")
                print("Enter 'c' to generate a combined report and delete individual reports.")
                print("Enter 'exit' to quit.")

                choice = input("Enter your choice: ").strip().lower()
                if choice.isdigit() and 1 <= int(choice) <= len(items):
                    selected_item = items[int(choice) - 1]
                    if isinstance(structure[selected_item], dict):
                        return display_structure(structure[selected_item], path + "/" + selected_item)
                    else:
                        print("Selected item is a file. Use 'r' or 'c' to generate a report.")
                elif choice == "b":
                    return
                elif choice == "r":
                    print("\nGenerate Report Options:")
                    print("1. Text format")
                    print("2. CSV format")
                    report_choice = input("Choose format: ").strip()
                    if report_choice == "1":
                        self.generate_report_parallel(output_format="text")
                    elif report_choice == "2":
                        self.generate_report_parallel(output_format="csv")
                    else:
                        print("Invalid choice. Returning to navigation.")
                elif choice == "c":
                    print("\nGenerate Combined Report Options:")
                    print("1. Text format")
                    print("2. CSV format")
                    report_choice = input("Choose format: ").strip()
                    if report_choice == "1":
                        self.generate_report_parallel(output_format="text")  # Generate parallel reports
                        self.combine_reports(output_dir="reports", final_report="combined_report.txt", format="text")  # Combine them
                        self.clean_up_reports(output_dir="reports", format="text")  # Delete individual reports
                    elif report_choice == "2":
                        self.generate_report_parallel(output_format="csv")  # Generate parallel reports
                        self.combine_reports(output_dir="reports", final_report="combined_report.csv", format="csv")  # Combine them
                        self.clean_up_reports(output_dir="reports", format="csv")  # Delete individual reports
                    else:
                        print("Invalid choice. Returning to navigation.")
                elif choice == "exit":
                    print("Exiting program.")
                    exit()
                else:
                    print("Invalid choice. Try again.")

        display_structure(folder_structure)

    def clean_up_reports(self, output_dir="reports", format="text"):
        """
        Delete individual chunked report files after combining them.
        Args:
            output_dir (str): Directory where the chunked reports are stored.
            format (str): Format of the chunked reports ('text' or 'csv').
        """
        try:
            for file_name in os.listdir(output_dir):
                if file_name.startswith("report_") and file_name.endswith(f".{format}"):
                    os.remove(os.path.join(output_dir, file_name))
            print(f"Cleaned up individual {format} reports in {output_dir}.")
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def combine_reports(self, output_dir="reports", final_report="combined_report.txt", format="text"):
        """
        Combine all chunked reports into a single numerized report.
        Args:
            output_dir (str): Directory where chunked reports are stored.
            final_report (str): Name of the final combined report.
            format (str): Format of the chunked reports ('text' or 'csv').
        """
        # Get the list of all chunked report files, sorted by their numeric ranges
        report_files = sorted(
            [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith("report_") and f.endswith(f".{format}")],
            key=lambda x: int(x.split("_")[1].split("-")[0])
        )

        combined_path = os.path.join(output_dir, final_report)
        serial_number = 1

        try:
            if format == "text":
                with open(combined_path, "w") as final:
                    # Write the header
                    final.write("S.No\tFile Name\tSize (bytes)\n")

                    # Iterate through each chunked report file
                    for report_file in report_files:
                        with open(report_file, "r") as chunk:
                            for line in chunk:
                                # Parse each line from the chunk report
                                if ": " in line:
                                    file_name, size = line.split(": ")
                                    # Write the formatted output to the combined report
                                    final.write(f"{serial_number}\t{file_name.strip()}\t{size.strip()}\n")
                                    serial_number += 1

            elif format == "csv":
                with open(combined_path, "w", newline="") as final:
                    writer = csv.writer(final)
                    # Write the header
                    writer.writerow(["S.No", "File Name", "Size (bytes)"])

                    # Iterate through each chunked report file
                    for report_file in report_files:
                        with open(report_file, "r") as chunk:
                            for line in chunk:
                                # Parse each line from the chunk report
                                if ": " in line:
                                    file_name, size = line.split(": ")
                                    # Write the formatted output to the combined report
                                    writer.writerow([serial_number, file_name.strip(), size.strip()])
                                    serial_number += 1

            print(f"Combined {format.upper()} report saved to: {combined_path}")
        except Exception as e:
            print(f"Error while combining reports: {e}")




    def generate_report_parallel(self, output_dir="reports", output_format="text"):
        """
        Generate a report of the files and their sizes using parallel processing.
        Args:
            output_dir (str): Directory to save chunked reports.
            output_format (str): Format of the report ('text' or 'csv').
        """
        os.makedirs(output_dir, exist_ok=True)
        keys = list(self.file_sizes.keys())

        def write_chunk(start, end):
            chunk = keys[start:end]
            chunk_file = os.path.join(output_dir, f"report_{start}-{end}.{output_format}")
            with open(chunk_file, "w") as f:
                for file_name in chunk:
                    f.write(f"{file_name}: {self.file_sizes[file_name]} bytes\n")

        with ThreadPoolExecutor() as executor:
            chunk_size = 1000
            futures = [
                executor.submit(write_chunk, i, min(i + chunk_size, len(keys)))
                for i in range(0, len(keys), chunk_size)
            ]
            for future in futures:
                future.result()  # Ensure all threads complete

        print(f"Reports saved to directory: {output_dir}")


