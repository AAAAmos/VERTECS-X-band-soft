SYNC_MARKER = b'\x1A\xCF\xFC\x1D'
VCDU_head = b'\x55\x40'
OPT_EXTRA_HEADER = 28      # Optical receiver adds 28 bytes at the beginning
OPT_EXTRA_TRAILER = 160    # ...and 160 bytes at the end of each packet
TX_HEADER_SIZE = 28        # Transmitter header after sync marker (2+3+1+22 = 28 bytes)
MAX_DATA_SIZE = 1087       # Payload size per packet
MAX_PACKET_NUMBER = 3e5    # Maximum number of packets in a file NEED TO BE CONFIRMED
MISSINGRATE_TOLERANCE = 50 # Tolerance for missing rate, if the missing rate is larger than this value, request for whole file.

output_IM_folder_path = "./optical/"
report_path = "./report/"

csv_header = 'Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness\n'
