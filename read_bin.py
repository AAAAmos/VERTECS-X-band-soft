import sys
import numpy as np
from astropy.io import fits
import pandas as pd

# import os
# import psutil
# def print_memory():
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
#     print(f"[Memory] {mem:.2f} MB")

def DF_tmp_data(file_name):
    
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
    
# file_name = './optical/opt_frame_0005_F20250109155612.bin'
file_name = sys.argv[1]
DF = DF_tmp_data(file_name)

IM = DF[DF['VCDU']=='IM']
HK = DF[DF['VCDU']=='HK']
DATA = IM['data'] #data
header_X = HK['data'] #header

data = bytes()
for i in range(len(IM)):
    data += DATA[i]
data_rs = data.rstrip(b'\0')
try:
    data_array = np.frombuffer(data_rs,dtype=np.uint16)  #create a numpy array from object(such as bytes or bytearrays)
    image_data = data_array.reshape(3003,3008)
except Exception as e:
    sys.exit(4)
    
# print_memory()

#check if the length of data_array is 3003*3008
# if len(data_array) == 3003*3008: 
#     image_data = data_array.reshape(3003,3008)
# else:
#     data_nan = np.full(3003*3008,np.nan)   
#     data_nan[:len(data_array)] = data_array
#     image_data = data_nan   #if not, fill NaN into data_array until the total length is 3003*3008

#write a text file
a = ['1\n','2\n','3\n']
f = open('./mock_header.txt','w')
f.writelines(a)  #write line by line
f.close()

#read the text
ff = open('./mock_header.txt','r')
information = ff.readlines()
header_S = []
for info in information:
    header_S.append(info.strip())  #delete the '\n' after element in a

#write image data and header information in fits file
hdu = fits.PrimaryHDU(image_data)  #fits.PrimaryHDU(data)
hdu.header['header1'] = header_S[0]
hdu.header['header2'] = header_S[1]
hdu.header['header3'] = header_S[2]

# file_name = input("input file name or enter empty to exit:\n")
file_name = file_name.split('/')[-1].split('.')[0]
#add the header information

#if filename isn't an empty string
if file_name != "":
    hdu.writeto(f'./img/{file_name.split("_")[-1]}_test.fits', overwrite=True)