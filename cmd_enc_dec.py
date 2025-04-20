import sys
import datetime

#file_name = 'F20540802065959.bin'
#id_start = 16000
#id_end = 19000
#name = 'IM' #'HK'/'IMG'

#######################################################################
def make_command(file_name,name,id_start,id_end):
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

    if name == 'HK':
        label=0
        out_name = label.to_bytes(1,'big')
        out_ids = id_start.to_bytes(2,'big') + id_end.to_bytes(2,'big')
    elif name == 'IM':
        label=1
        out_name = label.to_bytes(1,'big')
        out_ids = id_start.to_bytes(2,'big') + id_end.to_bytes(2,'big')
    elif name == 'OK':
        label=2
        out_name = label.to_bytes(1,'big')
        id_temp = 0
        out_ids = id_temp.to_bytes(2,'big') + id_temp.to_bytes(2,'big')
    elif name == 'Error':
        label=3
        out_name = label.to_bytes(1,'big')
        id_temp = 65535
        out_ids = id_temp.to_bytes(2,'big') + id_temp.to_bytes(2,'big')
    else:
        print('ERROR: unknown label')
        print(name)
        sys.exit(2)
        
    return out_date + out_name + out_ids

######################################################################
def decode_command(com):
    
    fname = datetime.datetime.fromtimestamp(int.from_bytes(com[:4],'big'))
    file_name = 'F'+f'{fname:%Y%m%d%H%M%S}'+'.dat'
    #UNIX timestamp
    
    if com[4] == 0:
        label = 'HK'
    elif com[4] == 1:
        label = 'IM'
    elif com[4] == 2:
        label = 'OK'
    elif com[4] == 3:
        label = 'Error'                
    else:
        label = 'UNKNOWN'
    id_start=int.from_bytes(com[5:7],'big')
    id_end=int.from_bytes(com[7:9],'big')
    return [file_name,label,id_start,id_end]
