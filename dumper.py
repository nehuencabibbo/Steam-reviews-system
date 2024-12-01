import os
import shutil

def append_files_to_directory(source_dir, target_dir, target_file_name):
    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)
    
    # Path to the target file
    target_file_path = os.path.join(target_dir, target_file_name)
    
    # Open the target file in append mode
    with open(target_file_path, 'a') as target_file:
        # Iterate over all files in the source directory
        for file_name in os.listdir(source_dir):
            file_path = os.path.join(source_dir, file_name)
            
            # Check if it's a file (not a directory)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as source_file:
                    # Read the content of the source file
                    content = source_file.read()
                    
                    # Append the content to the target file
                    target_file.write(content)
                    target_file.write('\n')  # Add a newline for separation

if __name__ == "__main__":
    source_directory = '/path/to/source/directory'
    target_directory = '/path/to/target/directory'
    target_file = 'combined_output.txt'
    
    append_files_to_directory(source_directory, target_directory, target_file)