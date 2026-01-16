'''
Read Data from JK_PB1A16S10P and many other newer JK BMS over BLE and publish on a MQTT Server 

This program is free software: you can redistribute it and/or modify it under the terms of the
GNU General Public License as published by the Free Software Foundation, either version 3 of the License,
or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <https://www.gnu.org/licenses/>

NO COMMERCIAL USE !!

Copyright 2025 Z.TOe toedci@gmail.com
------------------------------------------------------------------------------------------------------------------------
If you do any modification to this python script please SHARE with me, thank you!!!!
------------------------------------------------------------------------------------------------------------------------
'''
import sys
import threading
import asyncio
from bleak import BleakClient, BleakScanner,BleakError
from bleak.exc import BleakDBusError
from threading import Lock
from cursor import *
from registers import *
import time
import json
import paho.mqtt.client as paho
from datetime import datetime
import warnings
import struct
import os
import platform
import errno
import concurrent.futures
import itertools
import pickle

# Version
VERSION = "2.2.3"

MQTTIP ="192.168.1.11"
MQTTPORT = 1883
topic_prefix = "JK"
mqttClient = paho.Client


RETRY_ATTEMPTS = 15
RETRY_DELAY = 1  # seconds
TIMEOUT_RESPONSE = 10
SHORT_FRAME_LIMIT = 20
SLEEP = 300
WATCHDOG_TIMEOUT = SLEEP*3 # Maximum time allowed for scanning & connecting & sleeping (seconds)
WRITE_TIMEOUT = 5  # Timeout in seconds

MQTT = True
OUTPUT = True
jsonStr = ""
jsonDevInfo = {} #dictionary
jsonDevSetting = {}


# BLE things
DEVICE_NAMES = ["290120255005-00","101020220001-01", "101020220002-02"]
DEVICE_NAMES_LAST = [] #Keep track of which devices have been processed in the current run
#SERVICE_UUID = "ffe0"
SERVICE_UUID = "0000FFE0-0000-1000-8000-00805f9b34fb"
#CHAR_UUID = "ffe1"
CHAR_UUID = "0000FFE1-0000-1000-8000-00805f9b34fb"
MIN_FRAME_SIZE = 300
MAX_FRAME_SIZE = 320
CMD_HEADER = bytes([0xAA, 0x55, 0x90, 0xEB])
CMD_TYPE_DEVICE_INFO = 0x97  # 0x03: Device Information
CMD_TYPE_CELL_INFO = 0x96  # 0x02: Cell Information
CMD_TYPE_SETTINGS = 0x95  # 0x01: Settings


stop_searching = False
# ble_buffer = bytearray(MAX_FRAME_SIZE)
# ble_buffer_index = 0
capturing = False
waiting_for_device_info = False
waiting_for_cell_info = False
#waiting_for_settings = False
thread_lock = Lock()
short_frame_count = 0
last_response = bytearray()  # Store the last valid response
cursor_chars = "\/|-"
indx = 0

# Messages
GET_DEVICE_INFO = bytearray([0xaa, 0x55, 0x90, 0xeb, 0x97, 0x00, 0xdf, 0x52, 0x88, 0x67, 0x9d, 0x0a, 0x09, 0x6b, 0x9a, 0xf6, 0x70, 0x9a, 0x17, 0xfd])
GET_CELL_INFO =   bytearray([0xaa, 0x55, 0x90, 0xeb, 0x96, 0x00, 0x79, 0x62, 0x96, 0xed, 0xe3, 0xd0, 0x82, 0xa1, 0x9b, 0x5b, 0x3c, 0x9c, 0x4b, 0x5d])
GET_SETTINGS =    bytearray([0xAA, 0x55, 0x90, 0xEB, 0x95, 0x00, 0x79, 0x62, 0x96, 0xED, 0xE3, 0xD0, 0x82, 0xA1, 0x9B, 0x5B, 0x3C, 0x9C, 0x4B, 0x5D])

last_activity_time = time.time()
notification_queue = None  # Global queue for notifications


def log(color=f"{RESET}",owner="0",process="", message="",**kwargs):
    #------------------------------------------------------------------------------------------
    if OUTPUT: print(f"{color}[{owner}]{RESET}{process}: {message}",**kwargs)
#------------------------------------------------------------------------------------------

def calculate_crc(data):
    #------------------------------------------------------------------------------------------
    return sum(data) & 0xFF
#------------------------------------------------------------------------------------------

def connectMqtt():
    # ---------------------------------------------------------------------------------------
    global MQTT, mqttClient

    if MQTT:
        # Initialise MQTT if configured
        clientid = "my" + topic_prefix

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # Use API versioning correctly
            mqttClient = paho.Client(client_id=clientid)#, callback_api_version=paho.CallbackAPIVersion.VERSION1)

        # if config.mqtt.username != "":
        #     mqttClient.tls_set()  # <--- even without arguments
        #     mqttClient.username_pw_set(username="", password="")

        # intopic = config.mqtt.topic_prefix+"/set_register"  # todo deye5k ins config this is what coming in
        # mqttClient.on_message = on_mqtt_message
        try:
            mqttClient.connect(MQTTIP, MQTTPORT)
        except:
            log("","PI4","MQTT","can't connect to server!!")
            return -1
        #mqttClient.subscribe(intopic)
        mqttClient.loop_start()  # start the loopmqttClient.loop_start() #start the loop
        log("","PI4","MQTT",f"connected to server {MQTTIP}")
        return mqttClient
# --connectMqtt-------------------------------------------------------------------------------------------------------
def split_u8_pair(word_u16, hi_name, lo_name, out_dict,
                  hi_first=False, word_name=None, store_hex=False):
        #---------------------------------------------------------------------------------------------
    """
    Split a 16-bit Modbus register into two UINT8 values and store both bytes
    (and optionally the full word) into out_dict.

    hi_first=True means: hi_name <- high byte, lo_name <- low byte
    hi_first=False swaps them.
    """
    w = int(word_u16) & 0xFFFF
    hi = (w >> 8) & 0xFF
    lo = w & 0xFF

    # store full word too (optional)
    if word_name:
        out_dict[word_name] = f"0x{w:04X}" if store_hex else w

    # store the two bytes
    if hi_first:
        out_dict[hi_name] = hi
        out_dict[lo_name] = lo
    else:
        out_dict[hi_name] = lo
        out_dict[lo_name] = hi

#---------------------------------------------------------------------------------------------
def parse_JK_settings(raw_data, devicename):
    #---------------------------------------------------------------------------------------------
    global jsonStr, jsonDevInfo,DEVICE_NAMES_LAST, last_activity_time,jsonDevSetting
    
    jsonDevSetting["SETTINGS"] = "**********"

    for pos, name, fmt, coeff, unit in JKDeviceSettingsRegs:
        size = struct.calcsize(fmt)
        if pos + size <= len(raw_data):
            value = struct.unpack_from(fmt, raw_data, pos)[0] * coeff

            if pos == 0x0114:
                v = int(value) & 0xFFFF          # UINT16 erzwingen
                jsonDevSetting["SysConfigBits"] = v

                for i, cfgname in enumerate(sys_config_bits):
                    jsonDevSetting[cfgname] = 1 if (v & (1 << i)) else 0
                    
                    #if OUTPUT: print(f"{cfgname} {jsonDevSetting[cfgname]}")
            else:
                jsonDevSetting[name] = round(value, 3) if isinstance(value, float) else value


    # Add alarm flags as a sub-dictionary
    # if alarm_data:
    #     parsed_data["Alarm"] = alarm_data
    last_activity_time = time.time() #Update that something has happened

    return jsonDevSetting
#-------------------------------------------------------------------------------------------------

def parse_JK_celldata(raw_data, devicename):
    #---------------------------------------------------------------------------------------------
    global jsonStr, jsonDevInfo,DEVICE_NAMES_LAST, last_activity_time
    parsed_data = {}

    # # Add date and time as the first two items
    # parsed_data["date"] = datetime.now().strftime("%Y-%m-%d")
    # parsed_data["time"] = datetime.now().strftime("%H:%M:%S")

    #alarm_data = {}  # Dictionary to store alarm flags separately

    parsed_data["CELLDATA"] = "**********"

    for pos, name, fmt, coeff, unit in JKCellInfoRegisters:
        size = struct.calcsize(fmt)
        if pos + size <= len(raw_data):
            value = struct.unpack_from(fmt, raw_data, pos)[0] * coeff
            if pos == 0x0040:  
                value = bin(value).count('1')  # nr of cell is the count number of bits set
            if pos == 0x008C:  
                value = f"{value:032b}"  # Convert to a 32-bit binary string

            if pos == 0x00A0:
                parsed_data["ALARM"] = value 
                # Store alarm flags under "Alarm" sub-item
                for i, alarm_name in enumerate(alarm_flags):
                    #alarm_data[alarm_name] = 1 if (value & (1 << i)) else 0
                    parsed_data[alarm_name] = 1 if (value & (1 << i)) else 0
            else:
                parsed_data[name] = round(value, 3) if isinstance(value, float) else value

    # Add alarm flags as a sub-dictionary
    # if alarm_data:
    #     parsed_data["Alarm"] = alarm_data
    last_activity_time = time.time() #Update that something has happened

    # MERGE jsonDevInfo FIRST
    merged_dict = {
        **jsonDevInfo,
        **parsed_data,
        **jsonDevSetting
    }

    jsonStr = json.dumps(merged_dict, ensure_ascii=False, indent=4)
    if OUTPUT: print(jsonStr)
    if devicename not in DEVICE_NAMES_LAST: #If the processed device isnt processed then add it to the processed list.
        DEVICE_NAMES_LAST.append(devicename)
    if sorted(DEVICE_NAMES_LAST) == sorted(DEVICE_NAMES): #If all devices are processed then clear out the list so that it can restart
        DEVICE_NAMES_LAST = []
    if MQTT:
        try:
            mqttClient.publish((topic_prefix + "/" + devicename), jsonStr)
        except:
            log("",devicename,"PARSER","MQTT can't publish!!")
            return -1

    return merged_dict
#-------------------------------------------------------------------------------------------------
def parse_dev_settings(device_name,data):
    #---------------------------------------------------------------------------------------------
    """Parsing Cell Info Frame (0x02) based on JK-BMS specification"""
    log("",device_name, "PARSER", "Dev Settings Frame...")

    try:
        # Extract the frame counter (Position 5)
        frame_counter = data[5]
        log("",device_name,"PARSER", f"Frame Counter: {frame_counter}")

        parsed_result = parse_JK_settings(data[6:],device_name)
        # for key, value in parsed_result.items():
        #     if OUTPUT:
        #         print(f"[{LBLUE}{device_name}{RESET}] {key.ljust(15)}: {value}")
        return parsed_result

    except Exception as e:
        log("",device_name,"PARSER", f"Error parsing Device Setting Frame: {e}")
        return None
#-------------------------------------------------------------------------------------------------

def parse_cell_info(device_name,data):
    #---------------------------------------------------------------------------------------------
    """Parsing Cell Info Frame (0x02) based on JK-BMS specification"""
    log("",device_name, "PARSER", "Cell Info Frame...")

    try:
        # Extract the frame counter (Position 5)
        frame_counter = data[5]
        log("",device_name,"PARSER", f"Frame Counter: {frame_counter}")
        
        # Extract enabled cells bitmask (Positions 70-73)BIT[n] ist 1 und zeigt damit an, dass die Batterie vorhanden ist.
        enabled_cells = int.from_bytes(data[70:74], byteorder='little')
        enabled_cells = bin(enabled_cells).count('1')
        log("",device_name,"PARSER", f"Enabled CellNr: {enabled_cells}")

        parsed_result = parse_JK_celldata(data[6:],device_name)
        # for key, value in parsed_result.items():
        #     if OUTPUT:
        #         print(f"[{LBLUE}{device_name}{RESET}] {key.ljust(15)}: {value}")
        return parsed_result

    except Exception as e:
        log("",device_name,"PARSER", f"Error parsing Cell Info Frame: {e}")
        return None
#-------------------------------------------------------------------------------------------------

def parse_device_info(device_name, data):
    #-------------------------------------------------------------------------------------------------
    global jsonDevInfo

    data_bytes = data[6:]
    device_info = {}

       # Add date and time as the first two items
    device_info["date"] = datetime.now().strftime("%Y-%m-%d")
    device_info["time"] = datetime.now().strftime("%H:%M:%S")

    for pos, name, fmt, coeff, unit in JKDeviceInfoRegisters:
        size = struct.calcsize(fmt)

        # bounds check must use data_bytes
        if pos + size > len(data_bytes):
            continue

        raw = struct.unpack_from(fmt, data_bytes, pos)[0]

        # handle u8 pairs (these MUST be read as UINT16 words: fmt == 'H' or '<H'/'>'H)
        if pos in (0x00B2, 0x00D4, 0x00E4, 0x00E6, 0x0104, 0x0106):
            # ensure we have an int word
            word = int(raw) & 0xFFFF

            if pos == 0x00B2:
                split_u8_pair(word, "*UART1MPRTOLNbr", "*CANMPRTOLNbr", device_info, hi_first=False,word_name="0x00B2",store_hex=True)

            elif pos == 0x00D4:
                split_u8_pair(word, "*UART2MPRTOLNbr", "UART2MPRTOLEnable", device_info, hi_first=False,word_name="0x00D4",store_hex=True)

            elif pos == 0x00E4:
                split_u8_pair(word, "*LCDBuzzerTrigger", "*DRY1Trigger", device_info, hi_first=False,word_name="0x00E4",store_hex=True)

            elif pos == 0x00E6:
                split_u8_pair(word, "*DRY2Trigger", "UARTMPTLVer", device_info, hi_first=False,word_name="0x00E6",store_hex=True)

            elif pos == 0x0104:
                split_u8_pair(word, "*RCVTime", "*RFVTime", device_info, hi_first=False,word_name="0x0104",store_hex=True)

            elif pos == 0x0106:
                split_u8_pair(word, "CANMPTLVer", "RVD0", device_info, hi_first=False,word_name="0x0106",store_hex=True)

            continue  # don't fall through and also store 'name'

        # strings: decode, do NOT scale
        if fmt.endswith('s'):
            s = raw.rstrip(b'\x00').decode('utf-8', errors='ignore')
            s = ''.join(c for c in s if c.isprintable())
            device_info[name] = s
            continue

        # numeric: scale
        value = raw * coeff
        device_info[name] = value

    log("", device_name, "PARSER", "Device Info")
    jsonDevInfo = {"Device": device_name, **device_info}
    return jsonDevInfo

# ---------------------------------------------------------------------------------------
def save_Serializable_globals():
    global DEVICE_NAMES_LAST
    with open('serializable_globals.pkl', 'wb') as f:
        pickle.dump(DEVICE_NAMES_LAST, f) #Save the current processed list past a script restart
# ---------------------------------------------------------------------------------------
def restart_script():
# ---------------------------------------------------------------------------------------
    """Forcefully restart the script if it hangs."""
    log("","MAIN","WATCHDOG", "No activity detected! Restarting script...")
    save_Serializable_globals()
    if "autostart" in sys.argv:
        os.execv(sys.executable, ['python'] + sys.argv)
    else:
        os.execv(sys.executable, ['python'] + sys.argv + ['autostart'])
# ---------------------------------------------------------------------------------------

def watchdog_task():
# ---------------------------------------------------------------------------------------
    """Background thread that restarts the script if no activity is detected."""
    global last_activity_time
    while True:
        time.sleep(WATCHDOG_TIMEOUT)
        if time.time() - last_activity_time > WATCHDOG_TIMEOUT:
            restart_script()
# ---------------------------------------------------------------------------------------

async def ble_callback(client, data, device_name):
    #---------------------------------------------------------------------------------------------
    global indx,capturing,  stop_searching, waiting_for_device_info, waiting_for_cell_info, \
        ble_buffer, ble_buffer_index, last_activity_time #, waiting_for_settings
    
    try:
        with thread_lock:
            if not capturing:
                if data[:4] == b'\x55\xAA\xEB\x90':
                    capturing = True
                    ble_buffer = bytearray()
                    log(GREEN, device_name ,"CALLBACK", f"Start detected! {data[:4].hex().upper()} Msg type: {data[4]:02X}")       
                    log("",device_name, "CALLBACK","frame received" , end=" ")
                    last_activity_time = time.time() #Update that something has happened
                else:
                    #log(device_name, f"CALLBACK: Wrong header detected! {data[:4].hex().upper()}")
                    #if OUTPUT: print(end="*",flush=True)
                    if OUTPUT: 
                        sys.stdout.write(f"\r{cursor_chars[indx]}\r")  # Overwrite the same line
                        sys.stdout.flush()
                        indx = (indx + 1) % len(cursor_chars)  #
                    return
                
            try:
                #log(device_name,f"CALLBACK: extend data")
                ble_buffer.extend(data)
            except Exception as e:
                log("",device_name,"CALLBACK", f"Error while processing data: {e}")
                ble_buffer_index = 0
                capturing = False
                return
                
            ble_buffer_index = len(ble_buffer)
            if OUTPUT: print(ble_buffer_index, end="*",flush=True) 

            if ble_buffer_index > MAX_FRAME_SIZE:
                log(RED,device_name,"CALLBACK", f"data size:{ble_buffer_index}")
                ble_buffer_index = 0
                capturing = False
                return


            if MIN_FRAME_SIZE <= len(ble_buffer) <= MAX_FRAME_SIZE:
                # CRC Validation
                crc_calculated = calculate_crc(ble_buffer[:-1])
                crc_received = ble_buffer[-1]
                
                if crc_calculated != crc_received:
                    log("\n",device_name,"CALLBACK", f"{len(ble_buffer)} bytes, CRC Invalid: {crc_calculated:02X} != {crc_received:02X}")
                    ble_buffer_index = 0
                    capturing = False
                    return 0
                else:
                    log("\n",device_name,"CALLBACK", f"{ble_buffer_index} bytes, CRC Valid: {crc_calculated:02X} == {crc_received:02X}")

                message_type = ble_buffer[4]
                log("",device_name,"CALLBACK", f"Call PARSER {message_type:02X} devinfo:{waiting_for_device_info},cellinfo:{waiting_for_cell_info}")
                
                if message_type == 0x1 and waiting_for_cell_info:

                    log("",device_name,"CALLBACK", "parse SETTINGS !!!!!!!!*******!!!") #parse_device_info(device_name, ble_buffer)
                    #waiting_for_settings = False
                    parse_dev_settings(device_name, ble_buffer)#print(f"buffer: {ble_buffer}")
                    log("",device_name,"CALLBACK", "Device SETTING processed.")
                
                elif message_type == 0x3 and waiting_for_device_info:
                    parse_device_info(device_name, ble_buffer)
                    waiting_for_device_info = False
                    log("",device_name,"CALLBACK", "Device Info processed.")
                    
                elif message_type == 0x2 and waiting_for_cell_info:
                    parse_cell_info(device_name, ble_buffer)
                    waiting_for_cell_info = False
                    log("",device_name, "CALLBACK","Cell Info processed. Processing complete.")
                    
                    # Stop notifications when all processing is complete
                    try:
                        await asyncio.wait_for(client.stop_notify(CHAR_UUID), timeout=5)
                    except asyncio.TimeoutError:
                        log("",device_name, "CALLBACK","Timed out waiting to stop notifications.")
                    log("",device_name, "CALLBACK","Stopped notifications.")
                    stop_searching = True  

                else:
                    log("",device_name,"CALLBACK", f"NO PARSER {message_type:02X} / wait devinfo:{waiting_for_device_info}, cellinfo:{waiting_for_cell_info}")
                # Reset buffer
                ble_buffer_index = 0
                capturing = False
    
    except Exception as e:
        log("",device_name,"CALLBACK", f"{e}")
#---------------------------------------------------------------------------------------------


async def data_queue_task(client, data, device_name):
    #---------------------------------------------------------------------------------------------
    # Safely add data to the queue
    global notification_queue
    """Safely add BLE notification data to the queue."""
    #log("",device_name, f"[data_queue_task] Processing: {client}, {data},{device_name}")
    await notification_queue.put((client, data, device_name))
 #---------------------------------------------------------------------------------------------

async def processBLE(device):
    # ---------------------------------------------------------------------------------------
    """Handles BLE connection and data processing for a device."""
    global stop_searching, waiting_for_device_info, waiting_for_cell_info,notification_queue #waiting_for_settings
    address = device.address
    device_name = device.name
    stop_searching = False
    attempt = 0

    while attempt < RETRY_ATTEMPTS and not stop_searching:
        try:
            async with BleakClient(address) as client:
                if OUTPUT and client.is_connected: 
                    log("\n",device_name, f"BLE","Connected")
                    
                
                if client.is_connected:
                    # Create a task to handle notifications and queue them
                    await client.start_notify(CHAR_UUID, lambda sender, data: asyncio.create_task(data_queue_task(client, data, device_name)))
                    log("",device_name, f"BLE","Subscribed to notifications")
                else: 
                   log(RED,device_name, f"BLE","NOT Connected 0") 
                   break
                
                if client.is_connected:
                    log(YELLOW,device_name, f"BLE"," Sending Device Info request...wait for response")
                    waiting_for_device_info = True
                    await client.write_gatt_char(CHAR_UUID, GET_DEVICE_INFO)  
                    
                    timeout_counter = 0
                    while waiting_for_device_info and timeout_counter < TIMEOUT_RESPONSE:
                        await asyncio.sleep(1)
                        timeout_counter += 1
                    
                    if waiting_for_device_info and timeout_counter >= TIMEOUT_RESPONSE:
                        log(RED,device_name, f"BLE","Timeout waiting for Device Info response.")
                        try:
                            await asyncio.wait_for(client.stop_notify(CHAR_UUID), timeout=5)
                        except asyncio.TimeoutError:
                            log(RED,device_name, f"BLE","Timed out waiting to stop notifications.")
                        waiting_for_device_info = False
                        break
                
                if client.is_connected:
                    log(YELLOW,device_name, "BLE","Sending Cell Info request...wait for response")
                    waiting_for_cell_info = True
                    await client.write_gatt_char(CHAR_UUID, GET_CELL_INFO) #is also receives the device SETTINGS message_type == 0x1
                    
                    timeout_counter = 0
                    while waiting_for_cell_info and timeout_counter < TIMEOUT_RESPONSE:
                        await asyncio.sleep(1)
                        timeout_counter += 1
                    
                    if waiting_for_cell_info and timeout_counter >= TIMEOUT_RESPONSE:
                        log(RED,device_name, f"BLE","Timeout waiting for Cell Info response.")
                        try:
                            await asyncio.wait_for(client.stop_notify(CHAR_UUID), timeout=5)
                        except asyncio.TimeoutError:
                            log(RED,device_name, f"BLE","Timed out waiting to stop notifications.")
                        waiting_for_cell_info = False
                        break
                
                log("\n"+LYELLOW,device_name, "BLE","Successfully processed the device")
                return True
        except Exception:
            if not stop_searching:
                if OUTPUT: print(f"--{attempt + 1}", end="", flush=True)
            if attempt < RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY)
            attempt += 1
    
    if not stop_searching and not capturing:
        log("\n"+RED,device_name, "BLE",f"Failed to connect after {RETRY_ATTEMPTS} attempts.")
    return False
#---------------------------------------------------------------------------------------------


async def scan_and_process_devices(
    scan_timeout=5.0,
    retry_delay=2.0,
    max_total_time_per_device=120.0,   # seconds
    max_connect_attempts=5             # tries after device is found
):
    #---------------------------------------------------------------------------------------------
    """
    Sequential processing:
    For each device_name in DEVICE_NAMES (in order):
      - repeatedly scan until found (or timeout)
      - once found, retry connect/process until success (or max attempts / timeout)
    """
    global last_activity_time

    for device_name in DEVICE_NAMES:
        start_t = time.time()
        connect_attempts = 0

        while True:
            # --- timeout guard for this device ---
            elapsed = time.time() - start_t
            if max_total_time_per_device is not None and elapsed >= max_total_time_per_device:
                log(RED, device_name, "BLE",
                    f"timeout after {int(elapsed)}s -> skipping to next device")
                break

            # --- scan until found ---
            try:
                device = await BleakScanner.find_device_by_filter(
                    lambda d, adv: adv.local_name == device_name,
                    timeout=scan_timeout
                )
            except BleakDBusError as e:
                if "InProgress" in str(e):
                    log(RED, device_name, "BLE", "scan in progress. Restarting...")
                    await restart_script()
                    return False
                log(RED, device_name, "BLE", f"scan error: {e}")
                await asyncio.sleep(retry_delay)
                continue
            except BleakError as e:
                log(RED, device_name, "BLE", f"scan error: {e}")
                await asyncio.sleep(retry_delay)
                continue

            if device is None:
                log(RED, device_name, "BLE", "not found yet, retrying...")
                await asyncio.sleep(retry_delay)
                continue

            # --- found -> try connect/process ---
            connect_attempts += 1
            last_activity_time = time.time()
            log(GREEN, device_name, "BLE",
                f"found! connect/process attempt {connect_attempts}/{max_connect_attempts}")

            try:
                ok = await processBLE(device)

                # Treat None as success; if bool returned, respect it
                if ok is False:
                    log(RED, device_name, "BLE", "processBLE returned False")
                    if max_connect_attempts is not None and connect_attempts >= max_connect_attempts:
                        log(RED, device_name, "BLE", "max connect attempts reached -> skipping")
                        break
                    await asyncio.sleep(retry_delay)
                    continue

                log(GREEN, device_name, "BLE", "processed OK -> next device")
                break  # next device

            except Exception as e:
                log(RED, device_name, "BLE", f"connect/process failed: {e}")
                if max_connect_attempts is not None and connect_attempts >= max_connect_attempts:
                    log(RED, device_name, "BLE", "max connect attempts reached -> skipping")
                    break
                await asyncio.sleep(retry_delay)
                continue

    return True
#---------------------------------------------------------------------------------------------

async def notify_process_task():
    #---------------------------------------------------------------------------------------------
    global notification_queue
    log("","MAIN","NOTIFY PROCESSOR",f"Started processing... Loop ID: {id(asyncio.get_running_loop())}")

    while True:
        # Print queue size for debugging
        #print(f"Queue size: {notification_queue.qsize()}")
        
        if not notification_queue.empty():  # Only process if there is data
            client, data, device_name = await notification_queue.get()
            #print(f"[{device_name}] Processing data...")
            await ble_callback(client, data, device_name)
            notification_queue.task_done()  # Mark the task as done
        else:
            await asyncio.sleep(0.1)  # Sleep for a short time if the queue is empty
#---------------------------------------------------------------------------------------------


async def main():
# ---------------------------------------------------------------------------------------
    global waiting_for_device_info,waiting_for_cell_info,loop,last_activity_time #,waiting_for_settings,
    global notification_queue, DEVICE_NAMES_LAST

    if "autostart" in sys.argv: #Check if the script has bee automatically restarted and reloads the list of processed devices. This is important to not get stuck on the same devices.
        if os.path.exists('serializable_globals.pkl'):
            with open('serializable_globals.pkl', 'rb') as f:
                DEVICE_NAMES_LAST = pickle.load(f)
    waiting_for_device_info = False
    waiting_for_cell_info = False
    #waiting_for_settings = False

    loop = asyncio.get_running_loop()
    notification_queue = asyncio.Queue()  # ✅ Initialize queue first!

    log("","MAIN ","LOOP_ID",f"{id(asyncio.get_running_loop())}")

    # Now start the notification processor
    task = asyncio.create_task(notify_process_task())
    await asyncio.sleep(1)  # Allow notify_process_task to start

    #log("","MAIN",f"Queue size before put: {notification_queue.qsize()}")
    #test queue
    await notification_queue.put(("client0", "data0", "device_name0"))
    #log("","MAIN",f"Queue size after put: {notification_queue.qsize()}")

    if platform.system() == "Linux":
        s=os.system("sudo systemctl restart bluetooth")
    elif platform.system() == "Windows":
        s=os.system("net stop Bluetooth && net start Bluetooth") 
    log(LBLUE,"MAIN","", f"restart bluetooth {s}")
    await asyncio.sleep(5)

    while True:
        last_activity_time = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now().strftime("%H:%M:%S")
        log(LBLUE,"MAIN","", f"Scanning for devices...{today} {now}")
        
        try:
            # Enforce a watchdog timeout on the scanning & processing loop
            if not waiting_for_cell_info and not waiting_for_device_info : #and not waiting_for_settings
                result = await asyncio.wait_for(scan_and_process_devices(), timeout=WATCHDOG_TIMEOUT)
            else: 
                log(RED,"MAIN","", "timeout: waiting frames")
            
        except asyncio.TimeoutError:
            log("\n"+RED,"MAIN", "WATCHDOG","Timeout exceeded! Restarting BLE scan...")           
            #break
        
        log(LBLUE,"BLE","", "All devices processed !")
        
        s = int(SLEEP/2)
        if OUTPUT: print(end=""+"sleep for "+str(SLEEP)+"sec ")
        for x in range(s):
            #todo mqtt break
            #if is_onmessage: break
            if x % 2 == 0:
                if OUTPUT: print(end="--"+str(x*2), flush=True)
            time.sleep(2)
        log("\n"+LBLUE,"BLE","", "Restarting scan..."+today+" "+ now)
        waiting_for_cell_info = False
        waiting_for_device_info = False
        #waiting_for_settings = False
# ---------------------------------------------------------------------------------------

if __name__ == "__main__":
    #os.system("cls" if os.name == "nt" else "clear")
    if len(sys.argv) < 2:
        OUTPUT = True
    elif 0 <= int(sys.argv[1]) <= 1:  # print output
        if int(sys.argv[1]) == 1:
            OUTPUT = True
        else:
            OUTPUT = False
    else:
        if OUTPUT: print("usage: *.py Output=0/1")
        sys.exit(errno.EACCES)


    if OUTPUT: print(f"JK Data Parser v{VERSION}")
    
    if MQTT:
        while not connectMqtt():
            time.sleep(SLEEP)
    else:
        log("","MAIN","MQTT","NO,  >>>> is mqtt = 0  !!!!!!!!")

        # Start watchdog thread
    watchdog_thread = threading.Thread(target=watchdog_task, daemon=True)
    watchdog_thread.start()

    asyncio.run(main())
