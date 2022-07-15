#from google.cloud import storage
import paramiko
import os
from datetime import datetime, timedelta
import pandas as pd
import argparse
import re
#minus=sys.argv[1]

parser = argparse.ArgumentParser(description='Pull daily files from client server')
parser.add_argument( '-f', '--file_name', help='Name of the file to be pulled from client server')
parser.add_argument('-i', '--intradate', action = 'store_true', help='is this a intradate')
args = parser.parse_args()


#storage_client = storage.Client()
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

#Use this command on linux to change the ppk file command : puttygen id_dsa.ppk -O private-openssh -o id_dsa
ssh.connect('129.146.29.61', username='profimetricsneo',password='profimetrics#neo#bck#202109',key_filename='/home/datauser/sftp_key/id_dsa')

ftp = ssh.open_sftp()

today=datetime.today()

if args.intradate:
    name = args.file_name
else:
    name="IPO_1701_{}{:02d}{:02d}01.csv".format(today.year,today.month,today.day - 1)


files_in_sftp = ftp.listdir("/home/datauser/profimetrics_approved_prices/")
pattern = re.compile("IPO_1702_{}{:02d}{:02d}\d\d\.csv".format(today.year, today.month, today.day))

while True:
    if any(pattern.match(file) for file in files_in_sftp):


        try:
            ftp.get('/output/{}'.format(name),'/home/datauser/profimetrics_approved_prices/{}'.format(name))
        except:
            print('failed to download file {} from sftp server'.format(name))
            raise

        df=pd.read_csv('/home/datauser/profimetrics_approved_prices/{}'.format(name),sep=';',index_col=0)
        df['snapshot_date']=(pd.to_datetime('today') - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%m:%S")
        df.to_csv('/home/datauser/profimetrics_approved_prices/{}_1'.format(name),sep=';')


        try:
            os.system('rm /home/datauser/profimetrics_approved_prices/{}'.format(name))
            os.system('mv /home/datauser/profimetrics_approved_prices/{}_1 /home/datauser/profimetrics_approved_prices/{}'.format(name,name))
            os.system('mkdir -p /home/datauser/profimetrics_daily_files/profimetrics_approved_prices')
            os.system('mv /home/datauser/profimetrics_approved_prices/{} /home/datauser/profimetrics_daily_files/profimetrics_approved_prices/'.format(name))
        except:
            print("Failed to execute os commands")
            raise

        files_in_sftp.remove(name)
    else:
        break

    ftp.close()
    ssh.close()