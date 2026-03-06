import os
import shutil
import random

# Define paths based on our new structure
source_dir = "../../synthea/output/fhir" 
batch_dest_dir = "../raw_data"
streaming_dest_dir = "../streaming_source"
num_folders = 20

# print(os.listdir("../../synthea/output/fhir"))

# 1. Create the 20 sub-folders
for i in range(1, num_folders + 1):
    if i <= 10:
        os.makedirs(os.path.join(batch_dest_dir, f"folder_{i}"), exist_ok=True)
    else:
        os.makedirs(os.path.join(streaming_dest_dir, f"folder_{i}"), exist_ok=True)

# 2. Get all JSON files and shuffle them
all_files = [f for f in os.listdir(source_dir) if f.endswith('.json')]
random.shuffle(all_files)

# 3. Distribute the files
for index, file_name in enumerate(all_files):
    folder_num = (index % num_folders) + 1
    src_path = os.path.join(source_dir, file_name)
    
    if folder_num <= 10:
        dest_path = os.path.join(batch_dest_dir, f"folder_{folder_num}", file_name)
    else:
        dest_path = os.path.join(streaming_dest_dir, f"folder_{folder_num}", file_name)
        
    shutil.copy(src_path, dest_path)

print(f"Successfully split {len(all_files)} files into Batch (Folders 1-10) and Streaming (Folders 11-20).")