from concurrent.futures import ThreadPoolExecutor 
import paramiko
from datetime import datetime, timedelta
import os
import glob
#from dotenv import load_dotenv
import pandas as pd
#import chardet
#import csv

#load_dotenv()
hostname = "auditsa.com.mx"
username = "multimedios"
password = "M260L2t41M3d10$"
port = 22
local = "C:/Users/miguel.sanchez/Desktop/FTP-data-downloader-main/"


# Suerclasbe para mejorar la velocidad de descarga
class FastTransport(paramiko.Transport):
    def __init__(self, sock):
        super(FastTransport, self).__init__(sock)
        self.window_size = paramiko.common.MAX_WINDOW_SIZE
        self.packetizer.REKEY_BYTES = pow(2, 40)
        self.packetizer.REKEY_PACKETS = pow(2, 40)


def find_today_files() -> list:
    last_day = datetime.today().date() 
    ssh_conn = FastTransport((hostname, port))
    ssh_conn.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(ssh_conn)

    files = sftp.listdir_attr()
    files_with_dates = [(file.filename, datetime.fromtimestamp(file.st_mtime).date()) for file in files]
    today = [file for file in files_with_dates if file[1] == last_day]

    remote_files = []
    for remote_file in today:
        #if remote_file[0].endswith(".rar"):
        remote_files.append(remote_file[0]) 

    sftp.close()
    ssh_conn.close()

    return remote_files


def download_file(file_r):
    try:
        ssh_conn = FastTransport((hostname, port))
        ssh_conn.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(ssh_conn)

        media = "tv" if "Tv" in file_r else "radio"
        kind = "testigos" if "1674" in file_r else "comercial"

        if file_r.endswith(".txt"):
            #Intenta abrir el archivo de manera remota y hace un df con este
            try: 
                with sftp.open(file_r, "r") as ftp_file:
                    df = pd.read_csv(ftp_file, sep="|", low_memory=False, encoding_errors="replace")
            #Si falla, intenta descargarlo
            except:
                sftp.get(file_r, local + file_r)
                df = pd.read_csv(local + file_r, sep="|", encoding="utf-8-sig", encoding_errors="replace", low_memory=False)

            df["HIT_Fecha"] = pd.to_datetime(df["HIT_Fecha"], dayfirst=False)
            last_day = df["HIT_Fecha"].max() 

            # Divide el archivo en 7 días
            for i in range(0, 7):
                d = (last_day - timedelta(days=i)).date()
                filtered = df.query("HIT_Fecha == @d")
                filtered.to_csv(f"{local}{str(d)}_{media}_auditsa_{kind}.csv", index=False, mode='w', header=True, sep="|", encoding="utf-8-sig")

        if file_r.endswith(".rar"):
            sftp.get(file_r, local + file_r)
            ##time.sleep(220) ## Wait 200 seconds until the .rar file is download, then extract it.
            import rarfile
            rarfile.UNRAR_TOOL = 'C://Program Files//WinRAR//UNrar.exe' 
            with rarfile.RarFile(local + file_r) as rf:
                # Extraer todos los archivos en el directorio de destino
                rf.extractall(path=local)
                print(f'Archivo extraído en {local}')

            list_of_files = glob.glob(f'{local}*.txt') # gets lastest .txt
            latest_file = max(list_of_files, key=os.path.getctime)
            print(latest_file)
                
            data = pd.read_csv(latest_file, sep="|")
            data["HIT_Fecha"] = pd.to_datetime(data["HIT_Fecha"], dayfirst=False)
                
            try: 
                for n in range(1, 32):
                    filtered = data[data["HIT_Fecha"].dt.day == n]
                    if filtered.empty == False:
                        date = filtered["HIT_Fecha"].iloc[0].strftime('%Y-%m-%d')
                        filtered.to_csv(f"{local}{date}_{media}_auditsa_{kind}.csv", index = False, sep="|", encoding="utf-8-sig")

                print("csv files created from " + local + file_r)
            except:
                print("Error when creating csv files!")

        if os.path.isfile(local + file_r):
            os.remove(local + file_r)
            print("Deleted " + local + file_r)

        sftp.close()
        ssh_conn.close()

    except Exception as e:
        print(f"Exception: {e}")
        print(f"Exception type: {type(e).__name__}")


today_files = find_today_files()

#Ejecutar la funcion en paralelo con los 4 archivos que se subieron hoy
with ThreadPoolExecutor() as executor:
    executor.map(download_file, today_files)