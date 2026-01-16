# JK-BMS_BLE_READER
Read Data from JK_PB1A16S10P and many other newer JK BMS over BLE and publish on a MQTT Server. This script reads 3 JK in one round, just put all your DEVICE_NAMES for more.

This script tested on a Pi4 wiith bluetooth.

- Set your MQTT IP/Port in script or NO mqtt under "MQTT"
- Set your Bluetooth JK_BMS device names under "DEVICE_NAMES"
- Set SLEEP in seconds for polling intervall

MQTT Topic is "JK/devName"
If you tested on an other JK BMS than JK_PB1A16S10P, please let me know

V2.2.1
from NotANewNick
- Added stability for unhandled exception in BleakScanner()
- Added functions to make sure each device is checked
- Added function to update the last_activity_time on real actions
- Added persistence to the list of checked devices

V2.2.3
- all registers from modbusV1.1 are implemented (info,settings,data)
