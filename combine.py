'''
This script is used to read the re-downloaded missing packets (RMP) and merge them with the incomplete data (IC).
It reads the RMP files, identifies which ICs they belong to, and merges them with the ICs. (ongoing)
------Input------
1. Read the RMP files and the IC files.
2. Identify which ICs the RMP files belong to. (ongoing)
------Output------
1. If there are no missing packets in the re-combined file, the script will save the image data to the optical folder.
2. If there are missing packets in the re-combined file, the script will save the incomplete image data to the tmp folder, replace the original IC.
3. The script will output a report file to the report folder.
'''

import glob
import os
import subprocess
import sys
import pandas as pd

# import psutil
# def print_memory():
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
#     print(f"[Memory] {mem:.2f} MB")

def DF_tmp_data(file_name):
    
    '''
    Read the tmp data file and return a DataFrame containing the header information.
    Input:
        file_name: str
            The name of the tmp data file.
    Output:
        dataDF: DataFrame
            The DataFrame containing the header information.
    '''
    
    with open(file_name, 'rb') as f:
        mpduPackets = f.read().split(b'\x1A\xCF\xFC\x1D')[1:]

    headers = [[], [], []]
    for k, packet in enumerate(mpduPackets):
        # classify the packets by the VCDU header
        if packet[:2] == b'\x55\x40':
            headers[0].append('IM')
            headers[2].append(packet[5:])
        elif packet[:2] == b'\x40\x3F':
            headers[0].append('HK')
            headers[2].append(packet[5:])
        else:
            continue
        
        headers[1].append(int.from_bytes(packet[2:5], 'big'))
        
    headerDF = pd.DataFrame({
        'VCDU': headers[0],
        'PSC': pd.Series(headers[1], dtype=int),
        'data': pd.Series(headers[2], dtype='object')  # Preserve binary data
    })
    
    return headerDF

def DF_raw_data(file_name):
    
    '''
    Read the raw data file and return a DataFrame containing the header information.
    Input:
        file_name: str
            The name of the raw data file.
    Output:
        dataDF: DataFrame
            The DataFrame containing the header information.
    '''
    
    with open(file_name, 'rb') as f:
        mpduPackets = f.read().split(b'\x1A\xCF\xFC\x1D')[1:]

    headers = [[], [], [], [], []]
    for k, packet in enumerate(mpduPackets):
        # classify the packets by the VCDU header
        if packet[28:30] == b'\x55\x40':
            headers[0].append('IM')
            headers[4].append(packet[56:-160])
            # headers[4].append(0)
        elif packet[28:30] == b'\x40\x3F':
            headers[0].append('HK')
            headers[4].append(packet[56:-160])
            # headers[4].append(0)
        else:
            continue
        
        headers[1].append(int.from_bytes(packet[30:33], 'big'))
        headers[2].append(packet[34])
        headers[3].append(packet[1])
        
    dataDF = pd.DataFrame({
        'VCDU': headers[0],
        'PSC': pd.Series(headers[1], dtype=int),
        'IB': headers[2],
        'DQ': pd.Series(headers[3], dtype=int),
        'data': pd.Series(headers[4], dtype='object')  # Preserve binary data
    })
    
    return dataDF

def encode_data(filename, VCDU, PSC_DF, data_DF, mode, sync_bytes=b'\x1A\xCF\xFC\x1D'):
    '''
    Used for store incomplete data.
    ------Parameters------
    filename: str
        The name of the file to write to.
    VCDU: bytes
        The VCDU header for identifying the data.
    PSC_DF: DataFrame
        The DataFrame containing the PSC values.
    data_DF: DataFrame
        The DataFrame containing the data values.
    mode: str
        The mode to open the file in. 'wb' and 'ab'.
    sync_bytes: bytes
        The sync bytes to use to separate records.
    '''
    try:
        PSC_list = PSC_DF.values.tolist()
        data_list = data_DF.values.tolist()
        with open(filename, mode) as f: 
            for i in range(0, len(data_DF)):
                f.write(sync_bytes)
                f.write(VCDU)
                # Pack the sequence count into bytes
                PSC_bytes = PSC_list[i].to_bytes(3, byteorder='big')
                f.write(PSC_bytes)
                f.write(data_list[i])
            if mode == 'wb':
                print(f"Data write to {filename}")
            else:
                print(f"Data append to {filename}")
    except Exception as e:
         print(f"Error writing to file: {e}")

def find_consecutive_ranges(lst):
    
    '''
    Find the consecutive ranges in a list of integers.
    Input:
        lst: a list of integers
    Output:
        ranges: a list of lists, each sublist contains the start and end of a consecutive range
    '''
    
    if not lst:
        return []
    
    ranges = []
    start = lst[0]
    
    for i in range(1, len(lst)):
        if lst[i] != lst[i - 1] + 1:
            ranges.append([start, lst[i - 1]])
            start = lst[i]
    
    ranges.append([start, lst[-1]])  # Append the last range
    
    return ranges

status = 'test'

IM_mask = lambda x: (x['VCDU'] == 'IM')
HK_mask = lambda x: (x['VCDU'] == 'HK')
DQ_mask = lambda x: (x['DQ'] == 0)

output_IM_folder_path = "./optical/"
VCDU_image = b'\x55\x40'
VCDU_HK = b'\x40\x3F'
csv_header = 'Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness\n'

if status == 'test':
    tmp_files = glob.glob('./tmp/tmp_*.bin')
    mock_request = glob.glob('./requested_data/*.bin')
    requested_data = DF_raw_data(mock_request[-1])
else:
    requested_file = sys.argv[1]
    requested_data = DF_raw_data(requested_file)
    incomplete_files = list(set(requested_data['ID'])) # assumption
    tmp_files = []
    for id in incomplete_files:
        tmp_files.append(glob.glob(f'./tmp/tmp_*{id}*.bin')[0])
        if len(tmp_files) == 0:
            print(f'No tmp file found for {id}')
            continue

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

                