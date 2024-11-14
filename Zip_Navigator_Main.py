from utils_zip.zip_util import ZipInspector

file_url = "" # Replace with the actual ZIP file URL 
inspector = ZipInspector(file_url)
inspector.inspect()
inspector.navigate_and_display()
