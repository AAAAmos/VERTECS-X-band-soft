import glob
import os
import subprocess
import sys
import datetime
import pandas as pd

from constants import *
from utility import process_packet # for DF_raw_data
from utility import DF_raw_data
from utility import encode_data
from utility import find_consecutive_ranges # for find_missing_packets
from utility import find_totalpackets # for find_missing_packets
from utility import find_missing_packets
from utility import DF_tmp_data

output_IM_folder_path = "./optical/"

def main():
    
    requested_file = sys.argv[1]
    
    try:
        requested_Data = DF_raw_data(requested_file) # PLEASE CHECK the format of requested file!
        requested_files = list(set(requested_Data['Filename']))
        
        for id in requested_files:
            
            tmp_file = glob.glob(f'./tmp/tmp_{id}')[0]
            
            if len(tmp_file) == 0:
                print(f'Extracting required file {requested_file}. tmp file {id} not found.')
                continue
            
            tmp_data = DF_tmp_data(tmp_file)
            requested_data = requested_Data[requested_Data['Filename'] == id]
            
    
    except Exception as e:
        print(f"Error: {e}. Input file unknown.")
        sys.exit(3)



                