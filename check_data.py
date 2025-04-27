'''
This script reads the raw data, checks completeness of the data, and the data quality.
------Parameters------
file_name: the name of the raw data file to be checked (full path). 
    If no input, the last file in the raw_data folder will be checked.
------Output------
The script will write the report to the last report file (.csv) in the report folder.:
    - If there is no missing image packets, the script will save the image data to the optical folder.
    - If there are missing image packets, the script will store the incomplete image data to the tmp folder.
'''

import glob
import os
import sys
import pandas as pd

# import psutil
# def print_memory():
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
#     print(f"[Memory] {mem:.2f} MB")

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

def encode_data(filename, VCDU, PSC_DF, data_DF, mode, sync_bytes=b'\x1A\xCF\xFC\x1D'):
    '''
    Used for storing data. Only DQ=0 data will be stored.
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

def DF_raw_data(file_path):
    
    '''
    Read the raw data file and return a DataFrame containing the header information.
    Input:
        file_path: str
            The path of the raw data file.
    Output:
        dataDF: DataFrame
            The DataFrame containing the header information.
    '''
    
    with open(file_path, 'rb') as f:
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

output_IM_folder_path = "./optical/"
report_path = "./report/"
# get the file name to be checked
files = glob.glob('./raw_data/*.bin')
files.sort()
if len(sys.argv)<2:
    file_path = files[-1]
else:
    file_path = sys.argv[1]
file_name = file_path.split("/")[-1]

VCDU_image = b'\x55\x40'
VCDU_HK = b'\x40\x3F'
csv_header = 'Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness\n'

# read the raw data, split the data into packets using sync bytes
with open(file_path, 'rb') as f:
    mpduPackets = f.read().split(b'\x1A\xCF\xFC\x1D')[1:]

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
# print(f'Report file: {fout_name}')

headerDF = DF_raw_data(file_path)

# check the completeness of the data
try: 
    im_mask, hk_mask = headerDF['VCDU'] == 'IM', headerDF['VCDU'] == 'HK'
    IM, HK = headerDF[im_mask]['PSC'].astype(int), headerDF[hk_mask]['PSC'].astype(int)
    bad_IM, bad_HK = headerDF[(im_mask)&(headerDF['DQ'] != 0)]['PSC'].astype(int), headerDF[(hk_mask)&(headerDF['DQ'] != 0)]['PSC'].astype(int)
    IM_range = set(range(0, 16620))
    max_HK = HK[HK <= 8136].max() # observed, not confrimed by the document
    HK_range = set(range(int(min(HK)), int(max_HK)+1)) # if the number of HK is fixed, please change the range 
    IM = set(IM) - set(bad_IM)
    HK = set(HK) - set(bad_HK)
    missing_IM = IM_range - set(IM)
    missing_HK = HK_range - set(HK)
    missing_IM = sorted(list(missing_IM))
    missing_HK = sorted(list(missing_HK))
    missing_rate_IM = (len(missing_IM)/16621)*100
    missing_rate_HK = (len(missing_HK)/8000)*100 # 8k is for testing, not real
    missing_segment_IM = find_consecutive_ranges(list(missing_IM))
    missing_segment_HK = find_consecutive_ranges(list(missing_HK))

    IM_mask = lambda x: (x['VCDU'] == 'IM') & (x['DQ'] == 0)
    HK_mask = lambda x: (x['VCDU'] == 'HK') & (x['DQ'] == 0) # can be replaced by the packet type that store the fits header information in the future update.    
    if (missing_rate_IM == 0) and (len(missing_HK) == 0):
        # no missing packets, save the image data
        nfiles = len(glob.glob(output_IM_folder_path+'*.bin'))
        nfiles = str(nfiles).zfill(4)
        outfile = f'./optical/opt_frame_{nfiles}_{file_name}'  # output file name
        # write the image data to the optical folder
        encode_data(outfile, VCDU_image, headerDF[IM_mask(headerDF)]['PSC'], headerDF[IM_mask(headerDF)]['data'], 'wb')
        # append the HK data to the optical folder
        encode_data(outfile, VCDU_HK, headerDF[HK_mask(headerDF)]['PSC'], headerDF[HK_mask(headerDF)]['data'], 'ab')
        # output the report
        with open(fout_name_cpl, 'a') as f:
            f.write(f'{file_name},OK,0,0,0\n')
        # print_memory()
    else:
        outfile = f'./tmp/tmp_{file_name}'
        # store the incomplete image data
        encode_data(outfile, VCDU_image, headerDF[IM_mask(headerDF)]['PSC'], headerDF[IM_mask(headerDF)]['data'], 'wb')
        # append the incomplete HK data
        encode_data(outfile, VCDU_HK, headerDF[HK_mask(headerDF)]['PSC'], headerDF[HK_mask(headerDF)]['data'], 'ab')
        # output the report for the missing packets
        with open(fout_name_incpl, 'a') as f:
            for segment in missing_segment_IM:
                f.write(f'{file_name},IM,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')
            for segment in missing_segment_HK:
                f.write(f'{file_name},HK,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')

except Exception as e:
    # report for unreadable files
    with open(fout_name_incpl, 'a') as f:
        f.write(f'{file_name},Error,65535,65535,100\n')
    sys.exit(1)