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
import datetime
import pandas as pd
from collections import Counter

# import psutil
# def print_memory():
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
#     print(f"[Memory] {mem:.2f} MB")

SYNC_MARKER = b'\x1A\xCF\xFC\x1D'
VCDU_head = b'\x55\x40'
OPT_EXTRA_HEADER = 28      # Optical receiver adds 28 bytes at the beginning
OPT_EXTRA_TRAILER = 160    # ...and 160 bytes at the end of each packet
TX_HEADER_SIZE = 28        # Transmitter header after sync marker (2+3+1+22 = 28 bytes)
MAX_DATA_SIZE = 1087       # Payload size per packet
MAX_packet_number = 3e5    # Maximum number of packets in a file NEED TO BE CONFIRMED
invalid_vcdu_count = 0

Missing_rate_tolerance = 50 # Tolerance for missing rate, if the missing rate is larger than this value, request for whole file.
output_IM_folder_path = "./optical/"
report_path = "./report/"
# get the file name to be checked
files = glob.glob('./raw_data/*.bin')
files.sort()
file_path = sys.argv[1]
raw_file_name = file_path.split("/")[-1]

csv_header = 'Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness\n'

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
    lst = sorted(lst)
    start = lst[0]
    
    for i in range(1, len(lst)):
        if lst[i] != lst[i - 1] + 1:
            ranges.append([start, lst[i - 1]])
            start = lst[i]
    
    ranges.append([start, lst[-1]])  # Append the last range
    
    return ranges

def encode_data(filename, VCDU, PSC_DF, data_DF, mode, sync_bytes=b'\x1A\xCF\xFC\x1D'):
    '''
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

def process_packet(raw_packet):
    """

    Raw optical packet structure:
      [Optical Extra Header (28 bytes)] +
      [Transmitter Packet: (VCDU header (2) + sequence (3) + reserved (1) + MDPU header (22) + payload (MAX_DATA_SIZE))] +
      [Optical Extra Trailer (160 bytes)]
      
    The new MDPU header (22 bytes) is structured as follows:
      • Bytes  0-1: Reserved (unique ID as 2 bytes, from the first 2 bytes of the provided hex identifier)
      • Bytes  2-8: Destination callsign (7 bytes, e.g. "JG6YBW\x00")
      • Bytes  9-15: Unique identifier (4-byte hex derived from the provided hex identifier, padded with 3 null bytes)
      • Byte     16: Data type (1 byte)
      • Bytes 17-20: Actual file length (4 bytes, big-endian)
      • Byte     21: Packet type indicator (1 byte)
      
    This function processes a raw packet and extracts the relevant information.
    Input:
        raw_packet: bytes
            The raw packet data to process.
    Output:
        DQ: int
            The data quality indicator.
        seq: int
            The sequence number of the packet.
        ptype: bin
            The packet type indicator.
        actual_file_length: int
            The actual length of the file.
        payload: bytes
            The payload data of the packet.
        file_uid: str
            The unique identifier of the file.
    """
    global invalid_vcdu_count
    DQ = raw_packet[1]
    trimmed = raw_packet[OPT_EXTRA_HEADER:]
    transmitter_packet = trimmed[:-OPT_EXTRA_TRAILER]
    if len(transmitter_packet) < TX_HEADER_SIZE: # xxx ? MAX_DATA_SIZE
        return None

    vcdu = transmitter_packet[0:2]
    if vcdu != VCDU_head:
        invalid_vcdu_count += 1
        return None

    seq = int.from_bytes(transmitter_packet[2:5], 'big')
    mdpu_header = transmitter_packet[6:28]
    payload = transmitter_packet[28:28+MAX_DATA_SIZE]
    fname = datetime.datetime.fromtimestamp(int.from_bytes(mdpu_header[9:13],'big'))
    file_uid = fname.strftime('%Y%m%d%H%M%S')
    ptype = mdpu_header[21]
    actual_file_length = int.from_bytes(mdpu_header[17:21], 'big')
   
    return DQ, seq, ptype, actual_file_length, payload, file_uid

def DF_raw_data(file_path, dq_check=False):
    
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
        packet_chunks = f.read().split(SYNC_MARKER)[1:]

    DQ, seq, ptype, actual_file_length, payload, file_uid = [], [], [], [], []
    for chunk in packet_chunks:
        result = process_packet(chunk)
        if result is None:
            continue
        DQ.append(result[0])
        seq.append(result[1])
        if result[2] == 0x03:
            ptype.append('TXT')
        elif result[2] == 0x04:
            ptype.append('LOG')
        elif result[2] == 0x01:
            ptype.append('CSV')
        elif result[2] == 0x05:
            ptype.append('JPG')
        elif result[2] == 0x00:
            ptype.append('BIN')
        else:
            ptype.append('UNKNOWN')
        ptype.append(result[2])
        actual_file_length.append(result[3])
        payload.append(result[4])
        file_uid.append(result[5])
    
    # Create a DataFrame from the collected data
    dataDF = pd.DataFrame({
        'Filename': pd.Series(file_uid, dtype=int),
        'PSC': pd.Series(seq, dtype=int),
        'DQ': pd.Series(DQ, dtype=int),
        'Type': pd.Series(ptype),
        'Length': pd.Series(actual_file_length, dtype=int),
        'data': pd.Series(payload, dtype='object')  # Preserve binary data
    })
    
    if dq_check:
        return dataDF[dataDF['DQ'] == 1]
    else:
        return dataDF

def find_totalpackets(data):
    """
    Find the total number of packets in the raw data file.
    Input:
        data: DataFrame
    Output:
        total_packets: int
            The total number of packets in the file.
    """
    filenames = set(list(data['Filename']))
    Lengths = 0
    for filename in filenames:
        lengths = data[data['Filename'] == filename]['Length']
        if set(lengths) != 1:
            Lengths += 0
        else:Lengths += lengths.iloc[0]
    
    return Lengths
    
def find_missing_packets(file_path):
    
    data = DF_raw_data(file_path)
    Lengths = find_totalpackets(data)
    
    max_psc = data['PSC'].max()
    if max_psc > MAX_packet_number:
        n = list(data['PSC'])
        n.sort(reverse=True)
        for i in range(len(n)):
            if n[i]<= MAX_packet_number:
                max_psc = n[i]
                break
    else:
        pass
    
    if Lengths == 0: # nothing in the file
        return -1, 100
    elif Lengths >= max_psc: # every file is not completely missing 
        PSC = set(range(1, Lengths+1))
        missed = PSC - set(data['PSC'])
        missing_rate = (len(missed)/Lengths)*100
        return find_consecutive_ranges(list(missed)), missing_rate
    else: # Lengths < max_psc, at least one file is completely missing, can't find the true length of the file
        PSC = set(range(1, max_psc+1))
        missed = PSC - set(data['PSC'])
        missing_rate = (len(missed)/max_psc)*100
        return find_consecutive_ranges(list(missed)), missing_rate

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
    
    try:
        missing_seg, missing_rate = find_missing_packets(file_path)
    
        if missing_seg == -1:
            # the file is empty, report it
            with open(fout_name_incpl, 'a') as f:
                f.write(f'{raw_file_name},Error,65535,65535,100\n')
            sys.exit(1)
        elif missing_rate >= Missing_rate_tolerance:
            # the file is completely missing, report it
            with open(fout_name_incpl, 'a') as f:
                f.write(f'{raw_file_name},Error,65535,65535,100\n')
            sys.exit(1)
        elif missing_rate < Missing_rate_tolerance:
            outfile = f'./tmp/tmp_{raw_file_name}'
            # store the incomplete image data
            encode_data(outfile, VCDU_image, headerDF[IM_mask(headerDF)]['PSC'], headerDF[IM_mask(headerDF)]['data'], 'wb')
            # output the report for the missing packets
            with open(fout_name_incpl, 'a') as f:
                for segment in missing_segment_IM:
                    f.write(f'{raw_file_name},IM,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')
                    
    except Exception as e:
        # report for unreadable files
        with open(fout_name_incpl, 'a') as f:
            f.write(f'{raw_file_name},Error,65535,65535,100\n')
        sys.exit(1)
        

if __name__ == "__main__":
    main()

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
        outfile = f'./optical/opt_frame_{nfiles}_{raw_file_name}'  # output file name
        # write the image data to the optical folder
        encode_data(outfile, VCDU_image, headerDF[IM_mask(headerDF)]['PSC'], headerDF[IM_mask(headerDF)]['data'], 'wb')
        # append the HK data to the optical folder
        encode_data(outfile, VCDU_HK, headerDF[HK_mask(headerDF)]['PSC'], headerDF[HK_mask(headerDF)]['data'], 'ab')
        # output the report
        with open(fout_name_cpl, 'a') as f:
            f.write(f'{raw_file_name},OK,0,0,0\n')
        # print_memory()
    else:
        outfile = f'./tmp/tmp_{raw_file_name}'
        # store the incomplete image data
        encode_data(outfile, VCDU_image, headerDF[IM_mask(headerDF)]['PSC'], headerDF[IM_mask(headerDF)]['data'], 'wb')
        # append the incomplete HK data
        encode_data(outfile, VCDU_HK, headerDF[HK_mask(headerDF)]['PSC'], headerDF[HK_mask(headerDF)]['data'], 'ab')
        # output the report for the missing packets
        with open(fout_name_incpl, 'a') as f:
            for segment in missing_segment_IM:
                f.write(f'{raw_file_name},IM,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')
            for segment in missing_segment_HK:
                f.write(f'{raw_file_name},HK,{segment[0]},{segment[1]},{missing_rate_IM+missing_rate_HK}\n')

except Exception as e:
    # report for unreadable files
    with open(fout_name_incpl, 'a') as f:
        f.write(f'{raw_file_name},Error,65535,65535,100\n')
    sys.exit(1)