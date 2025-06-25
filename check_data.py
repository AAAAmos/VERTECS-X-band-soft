import glob
import os
import sys 
import datetime
import pandas as pd

from constants import *
from utility import process_packet # for DF_raw_data
from utility import DF_raw_data
from utility import find_consecutive_ranges # for find_missing_packets
from utility import find_missing_packets
from utility import encode_data

# get the file name to be checked
file_path = sys.argv[1]

def main():
    
    # create a output file if needed 
    # determine report file
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
    
    Data = DF_raw_data(file_path)
    
    filenames = set(list(Data['Filename']))
        
    for filename in filenames:
        
        try:
            data = Data[Data['Filename'] == filename]
            missing_seg, missing_rate = find_missing_packets(data)
    
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
                encode_data(outfile, data)
                # output the report for the missing packets
                with open(fout_name_incpl, 'a') as f:
                    for segment in missing_seg:
                        f.write(f'{filename},Missing,{segment[0]},{segment[1]},{missing_rate}\n')

            elif missing_rate == 0:
                # the file is complete, save the mission data
                from read_bin import compile_data
                compile_data(Data)
                # output the report for the complete file
                with open(fout_name_cpl, 'a') as f:
                    f.write(f'{filename},OK,0,0,0\n')
                        
        except Exception as e:
            # report for unreadable files
            with open(fout_name_incpl, 'a') as f:
                f.write(f'{filename},Error,65535,65535,100\n')
            sys.exit(1)
        
if __name__ == "__main__":
    main()
