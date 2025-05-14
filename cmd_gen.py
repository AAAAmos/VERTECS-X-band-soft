import datetime
import os
import glob
import pandas as pd
import sys
import cmd_enc_dec as myenc
import numpy as np
# import binascii
import csv

#NOTFIXED: not fixed part
#######################################################################
def main():
    #NOTFIXED_START
    #time used in file name in command list
    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    now = datetime.datetime.now(JST)

    #variables
    # cmd output file name
    cmd_out = './cmd/'
    #folder where report csv file is in
    folder_decode_out = './report/'
    #number of packets per 1 request file 
    N_request = 16621
    #upper limit of requested packet segments per single raw file    
    N_id =  5 
    #number of packet per 1 raw data (used to calculate rate) 
    total_packet = 16621    
    #if the fraction of requested packets is larger than this, all data is requested
    rate_for_all = 0.8*100

    #folders
    folder_cmd_list = './cmd/list/' 
    folder_cmd_list_cur = folder_cmd_list +  f'{now:%Y%m%d_%H%M%S}' + '/'
    #NOTFIXED_END

    file_name = folder_decode_out + 'un_gen.csv'
    try:
        list_packet_t = pd.read_csv(file_name).values.tolist() #csv /w header
    except:
        # Not triggered if no un_gen.csv file (no new data)
        return 0
    
    os.makedirs(folder_cmd_list_cur[:-1])
    # os.makedirs(folder_cmd_bin_cur[:-1])
    command_order(folder_decode_out,folder_cmd_list_cur,N_request,N_id,rate_for_all,total_packet,now)
    command_bin(folder_cmd_list_cur,cmd_out)
        
#######################################################################
def command_bin(folder_list,folder_cmd):
    #generate command for request
    
    # determine the ouptput file name
    cmd_report = glob.glob('./cmd/*.txt')
    cmd_report.sort()
    if len(cmd_report) == 0:
        dt_now = datetime.datetime.now()
        time_now = dt_now.strftime('%Y%m%d%H%M%S')
        fout_name = f'./cmd/cmd_report_0000_{time_now}.txt'
    else:
        fout_name = cmd_report[-1]
        if os.path.getsize(fout_name) > 33*1000-1: # one line is 33 bytes 
            print('The last cmd report file is too large, create a new one.')
            dt_now = datetime.datetime.now()
            time_now = dt_now.strftime('%Y%m%d%H%M%S')
            fout_name = f'./cmd/report_{str(len(cmd_report)).zfill(4)}_{time_now}.txt'
        else:
            print(f'Write to the cmd report file: {fout_name}')
    
    #############################
    # generate command for request
    #NOTFIXED_START
    files = glob.glob(folder_list + 'REQ*.csv') #list for request
    #NOTFIXED_END
    files.sort()
    
    if(len(files)>0):
        for file_name in files:
            list_packet_t = pd.read_csv(file_name).values.tolist()
            for lists in list_packet_t:
                # print(file_name, list_packet_t) # xxx
                out_cmd_b = myenc.make_command(lists[0],lists[1],lists[2],lists[3])
                with open(fout_name , 'a') as f:
                    # f.write('cb da'+str(binascii.hexlify(out_cmd_b, ' '))[2:-1]+'\n')
                    # python version < 3.8
                    f.write('cb da' + ' '.join(f'{b:02x}' for b in out_cmd_b) + '\n')

    # generate command for delete
    #NOTFIXED_START                
    files = glob.glob(folder_list + 'DEL*.csv')#list for delete
    #NOTFIXED_END    
    files.sort()
    
    if(len(files)>0):
        for file_name in files:
            list_packet_t = pd.read_csv(file_name).values.tolist()
            for lists in list_packet_t:
                out_cmd_b = myenc.make_command(lists[0],lists[1],0,0) #command for delete file (not fixed yet?)
                with open(fout_name , 'a') as f:
                    # f.write('cb da'+str(binascii.hexlify(out_cmd_b, ' '))[2:-1]+'\n')
                    # python version < 3.8
                    f.write('cb da' + ' '.join(f'{b:02x}' for b in out_cmd_b) + '\n')
                    
    #############################

#######################################################################
def command_order(fol_dout,fol_lis,N_req,N_id,rate_for_all,total_packet,now):
    file_name = fol_dout + 'un_gen.csv'
    #list of request    
    list_packet = []
    #list of all data request    
    list_for_all = []
    #list of OK packet    
    list_OK = []       
    #Categorized in each list
    list_packet_t = pd.read_csv(file_name).values.tolist() #csv /w header
    add_csv(fol_dout + 'report.csv',list_packet_t)    
    for pac_t in list_packet_t:
        if (pac_t[4] < rate_for_all and pac_t[1] != 'OK' and pac_t[1] != 'Error'):
            list_packet.append(pac_t)
        elif (pac_t[1] == 'OK'):
            list_OK.append(pac_t)
        elif (pac_t[4]>= rate_for_all or pac_t[1] == 'Error'):
            list_for_all.append(pac_t)
        else:
            print("ERROR in command_order: unknown category")
            sys.exit()
            
    #list of raw file
    file_id = set([row[0] for row in list_packet])
    #Summarized by raw file, list_packet_sum[i] is a list of packet with a same file name
    list_packet_sum = [[row for row in list_packet if row[0] == id] for id in file_id]
    #sort so that older file former
    sorted_list_pac = sorted(list_packet_sum, key = lambda x: x[0][0][1:-4])

    #NOTFIXED_START
    #shorten number of packets > N_id
    for i in range(len(sorted_list_pac)):
        if ( len(sorted_list_pac[i] ) > N_id ):
            sorted_list_pac[i] = list_shorten(sorted_list_pac[i],N_id)
    #NOTFIXED_END

    #make a list : number of packets < N_req
    len_req = 0
    com_list = []
    n_csv = 0
    for pac_t in sorted_list_pac:
        pac_t = add_request_rate(pac_t,total_packet)
        for i in range(len(pac_t)):
            len_req += pac_t[i][3] - pac_t[i][2] + 1
            if(len_req < N_req):
                com_list.append(pac_t[i])
            else:
                save_to_csv(fol_lis + 'REQ',n_csv,com_list)
                len_req = 0 
                com_list = []
                n_csv += 1
                i = i-1
    save_to_csv(fol_lis + 'REQ',n_csv,com_list)
    n_csv += 1

    #make a list : request all packet
    for pac_t in list_for_all:
        pac_t[2] = 0 #set packet start & end for all packet
        pac_t[3] = total_packet
        pac_t.append(1) #rate for request
        save_to_csv(fol_lis + 'REQ',n_csv,[pac_t])
        n_csv += 1

    #make a list: complete data
    n_csv = 0
    for pac_t in list_OK:
        pac_t.append(0) #rate for request
        save_to_csv(fol_lis + 'DEL',n_csv,[pac_t])
        n_csv += 1

    os.remove(file_name)
    

#######################################################################
#reduce the number of command by combining missed packets
#NOTFIXED_START
def list_shorten(lists,N_id):
    while (len(lists)>N_id):
        length_lists = np.array([lists[i+1][2]-lists[i][3] for i in range(len(lists)-1)])
        length_min = np.min(length_lists)
        i_merge = np.where(length_lists == length_min)[0][0]
        list_add = [lists[i_merge][0],lists[i_merge][1],lists[i_merge][2],lists[i_merge+1][3],lists[i_merge][4]]
        lists = lists[:i_merge] + [list_add] + lists[i_merge+2:]
    return lists
#NOTFIXED_END

#######################################################################
#add request rate per one raw file FymdHMS.bin
def add_request_rate(lists,total_packet):
    packet_lists = np.array([raw[3]-raw[2] for raw in lists])
    sum_packet = np.sum(packet_lists)
    for raw in lists:
        raw.append(sum_packet/total_packet) 
    return lists

#save list of request in csv format    
def save_to_csv(folder_name,n_c,data):
    #filename = folder_name + f'{now_t:%Y%m%d%H%M%S}' + '_{0:05}.csv'.format(n_c)
    filename = folder_name + '_{0:05}.csv'.format(n_c)
    with open(filename, mode='w', newline='') as file:
        file.write('Filename,Type,Start_Packet_number,End_Packet_number,Incompleteness,req_rate\n')
        writer = csv.writer(file)
        writer.writerows(data)   
                
def add_csv(file_name, new_data):
    try:
        df = pd.read_csv(file_name).values.tolist()
        df.append(new_data)
    except FileNotFoundError:
        df = new_data
    with open(file_name, mode = 'a', newline = '') as file:
        writer = csv.writer(file)
        writer.writerows(new_data)
#######################################################################
if __name__ == "__main__":
    main()