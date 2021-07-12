import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import paho.mqtt.client as mqtt
from pprint import pprint
import datetime

import logging

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s : %(message)s')

DATA_AVG_TIME = 10
BATCH_DATA_NUMBER = 4

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("jetflow-606e85d99dec.json", scope)

client = gspread.authorize(creds)

# sheet = client.open("AQDdata").sheet1  # Open the spreadhseet

# sheet = client.open("AQDdata")

sheet_ = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").sheet1

sheet_cfg = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("機器設定")
sheet_room1 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM1")
sheet_room2 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM2")
sheet_room3 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM3")
sheet_room4 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM4")
sheet_room5 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM5")
sheet_room6 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM6")
sheet_room7 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM7")
sheet_room8 = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M").worksheet("ROOM8")

MachineMac = sheet_cfg.row_values(2)
headers = sheet_room1.row_values(1)
pprint(MachineMac[0])

MachineMac_Sheet_Table={    MachineMac[0]:sheet_room1,
                            MachineMac[1]:sheet_room2,
                            MachineMac[2]:sheet_room3,
                            MachineMac[3]:sheet_room4,
                            MachineMac[4]:sheet_room5,
                            MachineMac[5]:sheet_room6,
                            MachineMac[6]:sheet_room7,
                            MachineMac[7]:sheet_room8,
                        }

# row = sheet.row_values(3)  # Get a specific row
# col = sheet.col_values(3)  # Get a specific column
# cell = sheet.cell(1,2).value  # Get the value of a specific cell

# insertRow = ["hello", 5, "red", "blue"]
# sheet.append_row(insertRow)  # Insert the list as a row at index 4
# sheet.inser(insertRow,4)
# sheet.insert_row(insertRow,5)

# book = client.open_by_key("1rXAzhcdQVtW7x1m_AXQO6tqbTiAjXymIx4i6e7NC09M")

# book.add_worksheet("test_sheet", 10, 10)


# sheet.update_cell(2,2, "CHANGED")  # Update one cell

# numRows = sheet.row_count  # Get the number of rows in the sheet

# pprint(row)
# AQDlist=[]
AQDlist={MachineMac[0]+"_DEV":[],
         MachineMac[1]+"_DEV":[],
         MachineMac[2]+"_DEV":[],
         MachineMac[3]+"_DEV":[],
         MachineMac[4]+"_DEV":[],
         MachineMac[5]+"_DEV":[],
         MachineMac[6]+"_DEV":[],
         MachineMac[7]+"_DEV":[]}
# AQDlist.['MAC'] = ''
# AQDlist.['SENSORDATA'] = ''

# pprint(AQDlist)
put_values ={MachineMac[0]+"_DEV":[],
         MachineMac[1]+"_DEV":[],
         MachineMac[2]+"_DEV":[],
         MachineMac[3]+"_DEV":[],
         MachineMac[4]+"_DEV":[],
         MachineMac[5]+"_DEV":[],
         MachineMac[6]+"_DEV":[],
         MachineMac[7]+"_DEV":[]}

# 當地端程式連線伺服器得到回應時，要做的動作
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # 將訂閱主題寫在on_connet中
    # 如果我們失去連線或重新連線時
    # 地端程式將會重新訂閱
    client.subscribe(MachineMac[0]+"_DEV")
    client.subscribe(MachineMac[1] + "_DEV")
    client.subscribe(MachineMac[2] + "_DEV")
    client.subscribe(MachineMac[3] + "_DEV")
    client.subscribe(MachineMac[4] + "_DEV")
    client.subscribe(MachineMac[5] + "_DEV")
    client.subscribe(MachineMac[6] + "_DEV")
    client.subscribe(MachineMac[7] + "_DEV")
    # client.subscribe("A8:03:2A:57:0C:E0_DEV")
    # client.subscribe("A8:03:2A:57:0C:FC_DEV")
    # client.subscribe("A8:03:2A:57:0C:CC_DEV")



# 當接收到從伺服器發送的訊息時要進行的動作
def on_message(client, userdata, msg):
    # 轉換編碼utf-8才看得懂中文
    # print(msg.topic+" "+ msg.payload.decode('utf-8'))
    try:
        decode_data = json.loads(msg.payload.decode('utf-8'))
    except Exception as e:
        logging.error("Json format error.", exc_info=True)
        logging.error(msg.payload)
        return

    if decode_data["CMD"] != 0 :
        return

    AQDlist[msg.topic].append(decode_data)
    # AQDlist[msg.topic].count()

    # pprint(AQDlist)
    # pprint(len(AQDlist[msg.topic]))
    if (len(AQDlist[msg.topic])>DATA_AVG_TIME):
        del AQDlist[msg.topic][0:DATA_AVG_TIME]

    logging.debug("AQDlist[%s] have %d of data."%(msg.topic, len(AQDlist[msg.topic])))



    # CH2O = 0.0
    # if (len(AQDlist[msg.topic])==DATA_AVG_TIME):
    #
    #     for i in range(0,DATA_AVG_TIME):
    #         CH2O = CH2O+AQDlist[msg.topic][i]['CH2O']
    #     CH2O = CH2O/DATA_AVG_TIME
    # else :
    #     CH2O = None
    #
    # CO2 = 0.0
    # if (len(AQDlist[msg.topic]) == DATA_AVG_TIME):
    #
    #     for i in range(0, DATA_AVG_TIME):
    #         CO2 = CO2 + AQDlist[msg.topic][i]['CO2']
    #     CO2 = CO2 / DATA_AVG_TIME
    # else:
    #     CO2 = None
    # pprint(CO2)
    #
    # TVOC = 0.0
    # if (len(AQDlist[msg.topic]) == DATA_AVG_TIME):
    #
    #     for i in range(0, DATA_AVG_TIME):
    #         TVOC = TVOC + AQDlist[msg.topic][i]['TVOC']
    #     TVOC = TVOC / DATA_AVG_TIME
    # else:
    #     TVOC = None
    # pprint(TVOC)

    my_dict = {}
    sensorlist=["Temp", "RH", "PM2p5", "PM10", "O3", "CO", "CO2", "TVOC", "CH2O", "NO2"]

    for i in sensorlist:
        x = i
        my_dict[x] = 0.0
        if (len(AQDlist[msg.topic]) >= DATA_AVG_TIME):

            for i in range(0, DATA_AVG_TIME):
                if AQDlist[msg.topic][i][x] != 65535:
                    if AQDlist[msg.topic][i][x] != 60001:
                        my_dict[x] = my_dict[x] + AQDlist[msg.topic][i][x]
                    else:
                        my_dict[x] = "error"
                        break
                else:
                    my_dict[x] = "None"
                    break
            if type(my_dict[x]) == type(0.0):
                my_dict[x] = my_dict[x] / DATA_AVG_TIME
        else:
            logging.debug("AQDlist[%s] is not enought" % (msg.topic))
            return
            # my_dict[x] = "None"
    logging.debug("AQDlist[%s]計算平均成功" % (msg.topic))
    # x = "RH"
    # my_dict[x] = 0.0
    # if (len(AQDlist[msg.topic]) == DATA_AVG_TIME):
    #
    #     for i in range(0, DATA_AVG_TIME):
    #         my_dict[x] = my_dict[x] + AQDlist[msg.topic][i][x]
    #     my_dict[x] = my_dict[x] / DATA_AVG_TIME
    # else:
    #     my_dict[x] = None


    my_dict['Datetime']=datetime.datetime.now((datetime.timezone(datetime.timedelta(hours=8)))).strftime("%Y/%m/%d %H:%M:%S")
    my_dict['MAC'] = msg.topic
    # pprint(my_dict)
    macNumber=0
    for mac in MachineMac:
        if (mac+"_DEV" == msg.topic):
            sheet = MachineMac_Sheet_Table[mac]
            break
        macNumber = macNumber + 1

    # headers = sheet.row_values(1)

    # pprint(headers)

    temp = []
    for h in headers:
        temp.append(my_dict[h])
            # print("h=")
            # print(h)

    put_values[msg.topic].append(temp)
    # print("len"+msg.topic+"="+str(len(put_values[msg.topic])))
    if(len(put_values[msg.topic])>=BATCH_DATA_NUMBER):
        try:
            sheet.append_rows(put_values[msg.topic], table_range='A1')
            logging.info("%s 資料上傳成功，時間:%s"%(msg.topic, datetime.datetime.now((datetime.timezone(datetime.timedelta(hours=8)))).strftime("%Y/%m/%d %H:%M:%S")))
        except Exception as e:
            # logging.warning(e)
            logging.error("Catch an exception.", exc_info=True)
            logging.error("mac=%s, datanumber=%d"%(msg.topic,len(put_values[msg.topic])))
            logging.error(put_values[msg.topic])
            return
        # except:
        #     print("sheet error")
        #     return
        del put_values[msg.topic][0:BATCH_DATA_NUMBER]
        if(len(put_values[msg.topic])>0):
            print("There is some data in put_value %d"%len(put_values[msg.topic]))
    else:
        logging.debug("%s,累計%d筆資料未上傳" % (msg.topic, len(put_values[msg.topic])))
    # spreadsheet.values_append(sheetName, {'valueInputOption': 'USER_ENTERED'}, {'values': put_values})

    # sheet.append_row(put_values,table_range='A1')
    # pprint(decode_data)
    # pprint(decode_data['CH2O'])


# 連線設定
# 初始化地端程式
client = mqtt.Client()

# 設定連線的動作
client.on_connect = on_connect

# 設定接收訊息的動作
client.on_message = on_message

# 設定連線資訊(IP, Port, 連線時間)
client.connect("172.104.75.37", 1883, 60)

# 開始連線，執行設定的動作和處理重新連線問題
# 也可以手動使用其他loop函式來進行連接
client.loop_forever()


