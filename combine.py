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
from utility import find_missing_packets
from utility import DF_tmp_data

output_IM_folder_path = "./optical/"

def main():
    
    if os.path.isfile('./report/un_gen.csv'):
        fout_name_incpl = './report/un_gen.csv'
    else:
        fout_name_incpl = './report/un_gen.csv'
        with open(fout_name_incpl, 'w') as f:
            f.write(csv_header)
    print(f'Report file: {fout_name_incpl}')
    if os.path.isfile('./report/final_check.csv'):
        fout_name_cpl = './report/final_check.csv'
    else:
        fout_name_cpl = './report/final_check.csv'
        with open(fout_name_cpl, 'w') as f:
            f.write(csv_header)
    print(f'Report file: {fout_name_cpl}')
    
    requested_file = sys.argv[1]
    
    try:
        requested_Data = DF_raw_data(requested_file) # PLEASE CHECK the format of requested file!
        requested_files = list(set(requested_Data['Filename']))
        
        for filename in requested_files:
            
            tmp_file = glob.glob(f'./tmp/tmp_{filename}')[0]
            
            if len(tmp_file) == 0:
                print(f'Extracting required file {requested_file}. tmp file {filename} not found.')
                continue
            
            # load the incomplete data from tmp file
            tmp_data = DF_tmp_data(tmp_file)
            # extract the requested data from requested_Data to fill the missing packets
            requested_data = requested_Data[requested_Data['Filename'] == filename]
            requested_data = requested_data.drop_duplicates(subset=['PSC'])
            requested_data = requested_data.sort_values(by='PSC')
            # append requested_data to tmp_data
            updated_data = pd.concat([tmp_data, requested_data], ignore_index=True)
            updated_data = updated_data.drop_duplicates(subset=['PSC'])
            updated_data = updated_data.sort_values(by='PSC')
            # check the integrity of the updated data
            missing_seg, missing_rate = find_missing_packets(updated_data)
            
            
            if missing_seg == -1:
                # the file is empty, report it
                with open(fout_name_incpl, 'a') as f:
                    f.write(f'{filename},Error,65535,65535,100\n')
                    
            elif missing_rate >= MISSINGRATE_TOLERANCE:
                # the file is completely missing, report it
                with open(fout_name_incpl, 'a') as f:
                    f.write(f'{filename},Error,65535,65535,100\n')

            elif missing_rate < MISSINGRATE_TOLERANCE:
                # save the incomplete file
                outfile = f'./tmp/tmp_{filename}'
                encode_data(outfile, updated_data)
                # output the report for the missing packets
                with open(fout_name_incpl, 'a') as f:
                    for segment in missing_seg:
                        f.write(f'{filename},Missing,{segment[0]},{segment[1]},{missing_rate}\n')

            elif missing_rate == 0:
                # the file is complete, save the mission data
                from read_bin import compile_data
                compile_data(updated_data)
                # output the report for the complete file
                with open(fout_name_cpl, 'a') as f:
                    f.write(f'{filename},OK,0,0,0\n')
            
    
    except Exception as e:
        print(f"Error: {e}. Input file unknown.")
        sys.exit(3)



                