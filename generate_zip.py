import zipfile
import os

def generate_zip(files, output_filename):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            if os.path.isfile(file):
                zipf.write(file, os.path.basename(file))
            else:
                print(f"Warning: {file} does not exist and will be skipped.")
    print(f"Created {output_filename} with specified files.")

# Specifying the full paths of the files to include in the zip
files_to_zip = [
    r"C:\Users\Sharadha Kasi\Downloads\entrypoint.py",
    r"C:\Users\Sharadha Kasi\Downloads\ldap1.py",
    r"C:\Users\Sharadha Kasi\Downloads\result_emr.py",
    r"C:\Users\Sharadha Kasi\Downloads\generate_zip.py"
]

# Defining the output zip file name with full path
output_zip = r"C:\Users\Sharadha Kasi\Downloads\lsdaprj1.zip"

generate_zip(files_to_zip, output_zip)