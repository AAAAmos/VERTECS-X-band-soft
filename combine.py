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
            tmp_file = glob.glob(f'./tmp/tmp_*_{id}.bin')[0]
            
            if len(tmp_file) == 0:
                print(f'Extracting required file {requested_file}. tmp file {id} not found.')
                continue
            
            tmp_data = DF_tmp_data(tmp_file)
            requested_data = requested_Data[requested_Data['Filename'] == id]
            
            
    
    except Exception as e:
        print(f"Error: {e}. Input file unknown.")
        sys.exit(3)


try:
    for file_name in tmp_files:

        print(f'Processing {file_name}')
        tmp_data = DF_tmp_data(file_name)

        IM_range = set(range(0, 16620))
        try:
            HK_range = set(range(tmp_data[HK_mask]['PSC'].min(), tmp_data[HK_mask]['PSC'].max()+1)) # if the number of HK is fixed, please change the range 
        except:
            HK_range = set(range(800, 8000))
            
        # print(HK_range) # xxx

        # find the missing/bad-quality packets
        missing_IM = IM_range - set(tmp_data[IM_mask(tmp_data)]['PSC'])
        missing_HK = HK_range - set(tmp_data[HK_mask(tmp_data)]['PSC'])
        requested_IM_PSC = requested_data[IM_mask(requested_data)&DQ_mask(requested_data)]['PSC'].isin(missing_IM)
        requested_HK_PSC = requested_data[HK_mask(requested_data)&DQ_mask(requested_data)]['PSC'].isin(missing_HK)
        requested_IM = requested_data[IM_mask(requested_data)&DQ_mask(requested_data)][requested_IM_PSC]
        requested_HK = requested_data[HK_mask(requested_data)&DQ_mask(requested_data)][requested_HK_PSC]
        # combine the data
        combined_IM = pd.concat([tmp_data[IM_mask(tmp_data)], requested_IM[['VCDU', 'PSC', 'data']]]).sort_values(by='PSC')
        combined_HK = pd.concat([tmp_data[HK_mask(tmp_data)], requested_HK[['VCDU', 'PSC', 'data']]]).sort_values(by='PSC')
        missing_IM = IM_range - set(combined_IM['PSC'])
        missing_HK = HK_range - set(combined_HK['PSC'])
        missing_IM = sorted(list(missing_IM))
        missing_HK = sorted(list(missing_HK))
        missing_rate_IM = (len(missing_IM)/16621)*100
        missing_rate_HK = (len(missing_HK)/8000)*100 # 8k is for testing, not real

        # determine the report file
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

        # find the missing segments
        missing_segment_IM = find_consecutive_ranges(list(missing_IM))
        missing_segment_HK = find_consecutive_ranges(list(missing_HK))
        if (len(missing_IM) == 0) and (len(missing_HK) == 0):
            # no missing packets, save the image data
            nfiles = len(glob.glob(output_IM_folder_path+'*.bin'))
            nfiles = str(nfiles).zfill(4)
            outfile = f'./optical/opt_frame_{nfiles}_{file_name.split("/")[-1][4:]}'  # output file name
            # write the image data to the optical folder
            encode_data(outfile, VCDU_image, combined_IM['PSC'], combined_IM['data'], 'wb')
            # append the HK data to the optical folder
            encode_data(outfile, VCDU_HK, combined_HK['PSC'], combined_HK['data'], 'ab')
            # output the report
            with open(fout_name_cpl, 'a') as f:
                f.write(f'{file_name.split("/")[-1][4:]},OK,0,0,0\n')
            subprocess.run(['rm', file_name])
            # print_memory()
        else:
            outfile = f'./tmp/{file_name.split("/")[-1]}'
            # store the incomplete image data. replace the original tmp file
            encode_data(outfile, VCDU_image, combined_IM['PSC'], combined_IM['data'], 'wb')
            # append the incomplete HK data
            encode_data(outfile, VCDU_HK, combined_HK['PSC'], combined_HK['data'], 'ab')
            # output the report for the missing packets
            with open(fout_name_incpl, 'a') as f:
                for segment in missing_segment_IM:
                    f.write(f'{file_name.split("/")[-1]},IM,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')
                for segment in missing_segment_HK:
                    f.write(f'{file_name.split("/")[-1]},HK,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')

except Exception as e:
    # print(f"Error: {e}. Input file unknown.")
    sys.exit(3)

                