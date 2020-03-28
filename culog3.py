#################################################################
#  get json data from CU and save to log file
#  Kangoo Feb.2019
#
# Usage: python culog.py xxx.xxx.xxx.xxx [-h]
#         xxx.xxx.xxx.xxx  - IP address of the CU.
#         [-h]             - print help content
#################################################################
import zmq
import requests
import json
import pandas as pd
import pytz
from datetime import datetime, timezone,timedelta
from time import time,sleep
import os,sys
import signal


JST = timezone(timedelta(hours=+9), 'JST')
VERSIONINFO = 'CU logger 0.3 2019/9/22 Kangoo'
CONNECTION_TIMEOUT = 30
EFF_V600 = 0.9932

# flag to exit when user pressed Ctrl+C
interrupted = False
# show log data on tty while saving to log file
show_on_tty = False

####################################################################
# Ctrl+C signal handler
####################################################################
def signal_handler(signal, frame):
    global interrupted
    interrupted = True

####################################################################
# Print help content
####################################################################
def printhelp():
    print('Usage: python culog.py xxx.xxx.xxx.xxx [-h][-s]')
    print('      xxx.xxx.xxx.xxx  - IP address of the CU.')
    print('      [-h]             - This help.')
    print('      [-s]             - show data on tty.')


####################################################################
# download xml structure file from the CU and get Mac-Sn mapping dict
# input parameters:  ip_address: the IP address of the CU
# return :  mac-sn mapping dict 
#           None if downloading failed.
# Note: use the defalut id/password to access the CU
####################################################################
def get_mac_sn_dict_from_CU(ip_address):
    xml_url = 'http://{}:8080/dumpstructure.lp'.format(ip_address)
    mac_sn_dict={}
    
    import xml.etree.ElementTree as ET
    try:
        xml_data = requests.get(xml_url, auth=('admin', 'password'),timeout=CONNECTION_TIMEOUT).content
        _station = ET.fromstring(xml_data.decode())
    except:
        return None
    for _inverter in _station:
        for _string in _inverter:
            for _converter in _string:
                _mac = _converter.attrib['mac'] if ('mac' in _converter.attrib) else ""
                _sn = _converter.attrib['sn'] if ('sn' in _converter.attrib) else ""
                if (len(_mac)>0) and (len(_sn)>0):
                    mac_sn_dict.update([(_mac,_sn)])
    
    if (len(mac_sn_dict)>0): 
        return mac_sn_dict
    else:
        return None

####################################################################
# save data from the buffer to log file
# input parameters: json_buffer: the buffer list 
#                  log_file_path: full file path of the log file
#                  mac_sn_dict: mac-sn mapping dict
# return :  n/a
# Note: the datatime value will be localized as Asia/Tokyo tz.
####################################################################

def save_to_csv(json_buffer,log_file_path,mac_sn_dict):
    json_to_csv = pd.concat(json_buffer)
    json_to_csv = json_to_csv.rename(columns={"Inverters.Strings.name": "String","Inverters.name": "Inverter", \
                                              "utc": "datetime"})
    json_to_csv['datetime']    = pd.to_datetime(json_to_csv['datetime'],unit='s') \
                            .dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
    json_to_csv['sn'] = json_to_csv['mac'].map(mac_sn_dict)


    json_to_csv['Time'] =""
    json_to_csv['Date'] =""
    json_to_csv['Pout'] =round(json_to_csv['vout']*json_to_csv['iout'],3)


    #alulating Iin1 for V600-1 models'
    if ('Iin1' not in json_to_csv.columns):
        #print('     calulating Iin1 for V600-1 models')
        json_to_csv['Pin2'] = round((json_to_csv['vin2'] * json_to_csv['iin2']),3)
        json_to_csv['Pin1'] = round((json_to_csv['Pout']/EFF_V600)- json_to_csv['Pin2'],3)
        json_to_csv['iin1'] = round(json_to_csv['Pin1']/json_to_csv['vin1'],3)

    json_to_csv['Pdiss'] =round(json_to_csv['vin1']*json_to_csv['iin1']+json_to_csv['vin2']*json_to_csv['iin2'] - \
                          json_to_csv['Pout'],3)
                          
    # convert format to electrical files.
    json_to_csv = json_to_csv[['mac','sn','Time','Date','datetime','String', \
                                'vin1','vin2','vout','iin1','iin2','iout','text','Pdiss','Pout' ]]

    json_to_csv.rename({'mac': 'Mac', 'sn': 'SN', 'datetime': 'Date_Time', 'String': 'Location',\
                        'vin1': 'Vin1', 'vin2': 'Vin2','vout': 'Vout',\
                         'iin1': 'Iin1','iin2': 'Iin2', 'iout': 'Iout', 'text': 'Text'}, axis=1, inplace=True)

    #json_to_csv = json_to_csv.set_index(['Date_Time','Mac'])

    #end
    
    # if file does not exist write header 
    if not show_on_tty:
        print("saving buffering data to csv.....")
    if not os.path.isfile(log_file_path): 
        json_to_csv.to_csv(log_file_path, header='column_names', index=False,date_format='%Y-%m-%d %H:%M:%S')
    else: # else it exists so append without writing the header
        json_to_csv.to_csv(log_file_path, mode='a', header=False,index=False,date_format='%Y-%m-%d %H:%M:%S')
 
def print_json(json_pd):
    print(json_pd.to_string())

####################################################################
# get Json data from the CU and save to log file
# input parameters:  ip_address: the IP address of the CU
# return :  n/a
####################################################################
def cu_logging(ip_address):
    global interrupted
    global show_on_tty

    #prepare zmq 
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://{}:9191".format(ip_address))
    subscriber.setsockopt(zmq.SUBSCRIBE,b'')
    subscriber.setsockopt(zmq.RCVTIMEO, 30000) 

    mac_sn_dict = get_mac_sn_dict_from_CU(ip_address)

    #zmq reading buffer control
    BUFFER_MAX_SIZE = 5
    json_recv_utc = 0
    no_new_data_recv_count = 0
    site_name = None
    json_buffer = []

    while not interrupted:
        try:
            message = subscriber.recv()
            json_data = json.loads(message.decode())
            json_pd = pd.io.json.json_normalize(json_data, ['Inverters','Strings', 'Converters'], \
                                                    ['Station',['Inverters','name'],['Inverters','Strings','name']])
        except zmq.ZMQError as e:
            print("Request timeout(30s). {:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            continue  
        except:
            print("Data decoding error. {:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            continue 

        if site_name == None: 
            #logging file information
            site_name =json_data['Station']
            file_name ='Culog-{}({})_{:%Y%m%d-%H%M%S}.csv'.format(site_name.replace(' ','_'),ip_address,datetime.now())
            log_file_path = os.path.normpath(os.path.join(os.getcwd(), file_name))

        if (json_data['SC']['newData'] and (len(json_pd) > 0)): 
            json_pd = json_pd[ (json_pd['utc'] > json_recv_utc)]
            #json_pd['utc']    = pd.to_datetime(json_pd['utc'],unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
            
            if len(json_pd) > 0:
                if not show_on_tty:
                    print('{}:received {} new records .'.format(site_name,len(json_pd)))
                else:
                    print_json(json_pd)
                json_buffer.append(json_pd)
                json_recv_utc = json_data['SC']['utc']
            else:
                print('{}:no updated records in received data.{}'.format(site_name,no_new_data_recv_count+1))
                no_new_data_recv_count +=1
        else:
            print('{}:no updated info in received data.{}'.format(site_name,no_new_data_recv_count+1))
            no_new_data_recv_count +=1
        
        #save buffering data to csv and flash buffer
        if (len(json_buffer)> BUFFER_MAX_SIZE) or ((no_new_data_recv_count > 100) and (len(json_buffer)>0)):
            save_to_csv(json_buffer,log_file_path,mac_sn_dict)
            no_new_data_recv_count = 0     
            json_buffer = []

    #save buffered data to csv before exit
    if len(json_buffer) > 0:
        save_to_csv(json_buffer,log_file_path,mac_sn_dict)
        no_new_data_recv_count = 0  
    
    subscriber.close()
    print("CU log has been saved to csv file:{}".format(os.path.basename(log_file_path)))

####################################################################
# MAIN 
# Usage: python culog.py xxx.xxx.xxx.xxx [-h][-s]
#         xxx.xxx.xxx.xxx  - IP address of the CU.
#         [-h]             - print help content
#         [-s]             - show data on tty 
####################################################################
def main(argv):
    global interrupted
    global show_on_tty

    print(VERSIONINFO)

    ip_address = None

    print(len(argv))
    #print(argv[0])
    #print(argv[1])
    #print(argv[2])
    # check argvs....
    if (len(argv) == 1):
        printhelp()
        exit(1)
    elif (len(argv) == 2):
        if argv[1].lower == '-h':
            printhelp()
            exit(0)
        else:
            ip_address = argv[1]
            show_on_tty = True
    elif (len(argv) == 3):
        ip_address = argv[1]
        show_on_tty = True
    else:
        printhelp()
        exit(1)


    print("*********************************************************")
    print("Start CU data logging. IP address is {}".format(ip_address))
    print("Press Ctrl+C to stop logging.")
    print("*********************************************************")
    interrupted = False
    signal.signal(signal.SIGINT, signal_handler)

    #start cu logging
    cu_logging(ip_address)

    print("*********************************************************")
    print("Program exit since interruption of user.")
    print("*********************************************************")
    exit(0)



if __name__ == '__main__':
    main(sys.argv)

#Reversion hist
#  0.1 2019/2/15  1st version
#  0.2 2019/7/29  add showontty and save log with Electrical File Format.
#  0.3 2019/9/22  add Iin1 calculation for old V600 model