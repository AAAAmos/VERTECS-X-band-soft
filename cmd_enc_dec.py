import sys
import datetime

#file_name = 'F20540802065959.bin'
#id_start = 16000
#id_end = 19000
#name = 'IM' #'HK'/'IMG'

#######################################################################
def make_command(file_name,id_start,id_end,N):
    # 4 bytes file name ,3 bytes sequence number, 2 bytes number packet from seq number, 1 byte how many times
    #F20YYMMDDhhmmss
    YY = int(file_name[3:5])
    MM = int(file_name[5:7])
    DD = int(file_name[7:9])
    hh = int(file_name[9:11])
    mm = int(file_name[11:13])
    ss = int(file_name[13:15])
    fname = datetime.datetime(2000+YY,MM,DD,hh,mm,ss)
    unix_time = int(fname.timestamp())
    out_date = unix_time.to_bytes(4,'big')

    #UNIX timestamp

    id = id_start.to_bytes(3,'big')
    number = id_end - id_start + 1
    id += number.to_bytes(2,'big')
    n = N.to_bytes(1,'big')
        
    return out_date + id + n

######################################################################
def decode_command(com):
    
    com = com[2:]
    fname = datetime.datetime.fromtimestamp(int.from_bytes(com[:4],'big'))
    file_name = f'{fname:%Y%m%d%H%M%S}'
    #UNIX timestamp

    id_start=int.from_bytes(com[5:8],'big')
    num=int.from_bytes(com[8:10],'big')
    N=int.from_bytes(com[10:11],'big')
    return [file_name,id_start,num,N]
