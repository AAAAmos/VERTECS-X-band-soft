import glob
import os
import subprocess
import sys
import datetime
import pandas as pd

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

def DF_tmp_data(file_name):
    
    '''
    Decode the temporary data file compiled by encode_data function.
    '''
    
    with open(file_name, 'rb') as f:
        mpduPackets = f.read().split(SYNC_MARKER)[1:]

    for packet in mpduPackets:
        fname = datetime.datetime.fromtimestamp(int.from_bytes(packet[:4],'big'))
        psc = int.from_bytes(packet[4:7], 'big')
        ptype = int.from_bytes(packet[7:8], 'big')
        length = int.from_bytes(packet[8:12], 'big')
        data = packet[12:]
        
    headerDF = pd.DataFrame({
        'Filename': pd.Series(fname, dtype=int),
        'PSC': pd.Series(psc, dtype=int),
        'Type': pd.Series(ptype, dtype=int),
        'Length': pd.Series(length, dtype=int),
        'data': pd.Series(data, dtype='object')  # Preserve binary data
    })
    
    return headerDF

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
    ptype = int.from_bytes(mdpu_header[21], 'big')
    actual_file_length = int.from_bytes(mdpu_header[17:21], 'big')
   
    return seq, ptype, actual_file_length, payload, file_uid

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
        packet_chunks = f.read().split(SYNC_MARKER)[1:]

    seq, ptype, actual_file_length, payload, file_uid = [], [], [], [], []
    for chunk in packet_chunks:
        result = process_packet(chunk)
        if result is None:
            continue
        seq.append(result[0])
        ptype.append(result[1])
        actual_file_length.append(result[2])
        payload.append(result[3])
        file_uid.append(result[4])
    
    # Create a DataFrame from the collected data
    dataDF = pd.DataFrame({
        'Filename': pd.Series(file_uid, dtype=int),
        'PSC': pd.Series(seq, dtype=int),
        'Type': pd.Series(ptype, dtype=int),
        'Length': pd.Series(actual_file_length, dtype=int),
        'data': pd.Series(payload, dtype='object')  # Preserve binary data
    })
    
    return dataDF
    return dataDF

def encode_data(filename, data, sync_bytes=SYNC_MARKER):
    '''
    Store the incomplete data into a binary file at ./tmp/ with the specified format.
    Input:
        filename: str
            The name of the file to write the data to.
        data: DataFrame
            The DataFrame containing the data to be written.
        sync_bytes: bytes
            The sync bytes to be written at the beginning of each packet.
    '''
    try:
        fname_list = data['Filename'].values.tolist()
        PSC_list = data['PSC'].values.tolist()
        type_list = data['Type'].values.tolist()
        length_list = data['Length'].values.tolist()
        data_list = data['data'].values.tolist()
        with open(filename, 'wb') as f: 
            for i in range(0, len(data_list)):
                f.write(sync_bytes)
                # Convert the filename to a datetime object and then to Unix time
                YYYY = int(fname_list[i][:4])
                MM = int(fname_list[i][4:6])
                DD = int(fname_list[i][6:8])
                hh = int(fname_list[i][8:10])
                mm = int(fname_list[i][10:12])
                ss = int(fname_list[i][12:14])
                fname = datetime.datetime(YYYY,MM,DD,hh,mm,ss)
                unix_time = int(fname.timestamp())
                fname_bytes = unix_time.to_bytes(4, byteorder='big')
                f.write(fname_bytes)
                PSC_bytes = PSC_list[i].to_bytes(3, byteorder='big')
                f.write(PSC_bytes)
                Type_bytes = type_list[i].to_bytes(1, byteorder='big')
                f.write(Type_bytes)
                Length_bytes = length_list[i].to_bytes(4, byteorder='big')
                f.write(Length_bytes)
                f.write(data_list[i])
            print(f"Data write to {filename}")
            
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
    lst = sorted(lst)
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

                