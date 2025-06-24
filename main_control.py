import time 
import datetime
import os 
import subprocess
import glob
import sys

# import numpy as np
# import pandas as pd
# import binascii
# import csv
# from astropy.io import fits

# memory test
# import psutil
# def print_memory():
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
#     print(f"[Memory] {mem:.2f} MB")

# Check for new files every x seconds
check_time = 5 
csv_header = 'Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness\n'

raw_data_folder = "./raw_data/"
req_data_folder = "./requested_data/"
log_folder = "./log/"
archive_raw_folder = "./archive/raw_data/"
archive_req_folder = "./archive/requested_data/"
os.makedirs(log_folder, exist_ok=True)
os.makedirs(raw_data_folder, exist_ok=True)
os.makedirs(req_data_folder, exist_ok=True)
os.makedirs('./report/', exist_ok=True)
os.makedirs('./fits/', exist_ok=True)
os.makedirs('./csv/', exist_ok=True)
os.makedirs('./txt/', exist_ok=True)
os.makedirs('./log/', exist_ok=True)
os.makedirs('./jpg/', exist_ok=True)
os.makedirs('./tmp/', exist_ok=True)
os.makedirs('./cmd/', exist_ok=True)
os.makedirs('./cmd/list/', exist_ok=True)
os.makedirs(archive_raw_folder, exist_ok=True)
os.makedirs(archive_req_folder, exist_ok=True)

time_now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
nfiles = len(glob.glob(log_folder + "*.log"))
log_file = log_folder + f"log_{nfiles}_{time_now}.log"
subprocess.run(['touch', log_file])

cmd_gen_files = './report/un_gen.csv'
check_file = './report/final_check.csv'
final_report = './report/report.csv'
if os.path.exists(final_report):
    pass
else:
    with open(final_report, 'w') as f:
        f.write(csv_header)

while True:
    # print_memory()
    
    processed_raw_files = set()
    processed_req_files = set()
    processed_img_files = set()
    
    if os.path.getsize(log_file) > 1e7: # size limit of a report file is ~ 10MB
        print('The last log file is too large, create a new one.')
        dt_now = datetime.datetime.now()
        time_now = dt_now.strftime('%Y%m%d%H%M%S')
        nfiles = len(glob.glob(log_folder + "*.log"))
        log_file = log_folder + f"log_{nfiles}_{time_now}.log"
    else:
        print(f'Write to the log file: {log_file}')
    
    # Get new files, not including folders
    current_raw_files = {f for f in os.listdir(raw_data_folder) if os.path.isfile(os.path.join(raw_data_folder, f))}
    new_raw_files = current_raw_files - processed_raw_files
    current_req_files = {f for f in os.listdir(req_data_folder) if os.path.isfile(os.path.join(req_data_folder, f))}
    new_req_files = current_req_files - processed_req_files

    # call check_data.py
    for file in sorted(new_raw_files):  # Process in order
        file_path = os.path.join(raw_data_folder, file)
        with open(log_file, "a") as f:
            f.write(f"Checking {file}\n")
        try:
            subprocess.run(["python3", "./check_data.py", file_path], check=True)
            with open(log_file, "a") as f:
                f.write(f"Finish checking {file}\n")
            processed_raw_files.add(file)
        except Exception as e:
            with open(log_file, "a") as f:
                f.write(f"Error for checking {file_path}: {e}\n")
                f.write(f"Delete {file}, request again.\n")
            subprocess.run(['rm', file_path])
            continue
            
    # print_memory()
    
    # call combine.py
    for file in sorted(new_req_files):  # Process in order
        file_path = os.path.join(req_data_folder, file)
        with open(log_file, "a") as f:
            f.write(f"Extract packets from {file}\n")
        try:
            subprocess.run(["python3", "./combine.py", file_path], check=True)
            with open(log_file, "a") as f:
                f.write(f"Finished extracting {file}\n")
            processed_req_files.add(file)
        except Exception as e:
            with open(log_file, "a") as f:
                f.write(f"Error for extracting {file_path}: {e}\n")
                f.write(f"Delete {file}.\n")
            subprocess.run(['rm', file_path])
            continue
    
    # print_memory()

    # call cmd_gen.py
    subprocess.run(['python3', './cmd_gen.py'])
    
    # print_memory()
    
    time.sleep(check_time)
    
    # clear the processed files
    for file in processed_raw_files:
        with open(log_file, "a") as f:
            f.write(f"Move {file} to archive\n")
        subprocess.run(['mv', f'{raw_data_folder}{file}', f'{archive_raw_folder}{file}'])
    for file in processed_req_files:
        with open(log_file, "a") as f:
            f.write(f"Move {file} to archive\n")
        subprocess.run(['mv', f'{req_data_folder}{file}', f'{archive_req_folder}{file}'])
    # print(f'files: {current_files}')
    
    # print_memory()

