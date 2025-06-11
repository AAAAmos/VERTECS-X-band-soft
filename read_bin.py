def compile_data(DATA_df):
    
    import sys
    import os
    import numpy as np
    from astropy.io import fits
    
    files = set(list(DATA_df['Filename']))
    
    for file_name in files:

        This_file = DATA_df[DATA_df['Filename']==file_name]  #get the row of the dataframe where Filename is file_name
        if This_file['Type'].values[0] == 0:
            Type = 'fits'
        elif This_file['Type'].values[0] == 1:
            Type = 'csv'
        elif This_file['Type'].values[0] == 2:
            Type = 'mix'
        elif This_file['Type'].values[0] == 3:
            Type = 'txt'
        elif This_file['Type'].values[0] == 4:
            Type = 'log'
        elif This_file['Type'].values[0] == 5:
            Type = 'jpg'
        elif This_file['Type'].values[0] == 6:
            Type = 'H624'
        else:
            sys.exit(4)
            
        file_path = f'./Mission_data/{Type}/{file_name}.{Type}'
        
        if os.path.exists(file_path):
            continue  #if the file already exists, skip to the next file
            
        DATA = This_file['DATA'].values.tolist()  #get the DATA column from the dataframe
        length = This_file['Length'].values[0]  #get the Length column from the dataframe
        data = bytes()
        for i in range(len(DATA)):
            data += DATA[i]
        # data_rs = data.rstrip(b'\0')
        data_rs = data[:length]  #get the first 'length' bytes from data
        
        if Type == 'fits':
            try:
                data_array = np.frombuffer(data_rs,dtype=np.uint16)  #create a numpy array from object(such as bytes or bytearrays)
                image_data = data_array.reshape(3003,3008)
                
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

                #add the header information

                #if filename isn't an empty string
                if file_name != "":
                    hdu.writeto(file_path, overwrite=True)  #write the fits file to the img folder
            except Exception as e:
                sys.exit(4)
        else:
            try:
                with open(file_path, 'wb') as f:
                    f.write(data_rs)  #write the data to the file
            except Exception as e:
                sys.exit(4)
            
    