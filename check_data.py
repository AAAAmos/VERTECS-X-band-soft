import glob
import os
import sys
import datetime
import pandas as pd

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
        # if result[2] == 0x03:
        #     ptype.append('TXT')
        # elif result[2] == 0x04:
        #     ptype.append('LOG')
        # elif result[2] == 0x01:
        #     ptype.append('CSV')
        # elif result[2] == 0x05:
        #     ptype.append('JPG')
        # elif result[2] == 0x00:
        #     ptype.append('BIN')
        # else:
        #     ptype.append('UNKNOWN')
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
    
def find_missing_packets(data):
    """
    Find the missing packets in the raw data file.
    Input:
        data: DataFrame
            The DataFrame containing the raw data.
    Output:
        missing_segments: list
            A list of tuples, each containing the start and end of a missing packet range.
        missing_rate: float
            The percentage of missing packets in the file.
    """
    
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

def encode_data(filename, data, sync_bytes=SYNC_MARKER):
    '''
    Encode the data into a binary file with the specified format.
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
                fname_bytes = fname_list[i].to_bytes(4, byteorder='big')
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
        Data = DF_raw_data(file_path)
        missing_seg, missing_rate = find_missing_packets(Data)
    
        if missing_seg == -1:
            # the file is empty, report it
            with open(fout_name_incpl, 'a') as f:
                f.write(f'{raw_file_name},Error,65535,65535,100\n')
        elif missing_rate >= Missing_rate_tolerance:
            # the file is completely missing, report it
            with open(fout_name_incpl, 'a') as f:
                f.write(f'{raw_file_name},Error,65535,65535,100\n')
        elif missing_rate < Missing_rate_tolerance:
            outfile = f'./tmp/tmp_{raw_file_name}'
            # store the incomplete mission data
            encode_data(outfile, DF_raw_data(file_path))
            # output the report for the missing packets
            with open(fout_name_incpl, 'a') as f:
                for segment in missing_seg:
                    f.write(f'{raw_file_name},Missing,{segment[0]},{segment[1]},{missing_rate}\n')
        elif missing_rate == 0:
            # the file is complete, save the mission data
            from read_bin import compile_data
            compile_data(Data)
            with open(fout_name_cpl, 'a') as f:
                f.write(f'{raw_file_name},OK,0,0,0\n')
                    
    except Exception as e:
        # report for unreadable files
        with open(fout_name_incpl, 'a') as f:
            f.write(f'{raw_file_name},Error,65535,65535,100\n')
        sys.exit(1)
        
if __name__ == "__main__":
    main()
