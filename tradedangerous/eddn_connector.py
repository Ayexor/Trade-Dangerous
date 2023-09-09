#!/usr/bin/env python3
# Based on https://github.com/EDCD/EDDN/blob/master/examples/Python%203.4/Client_Complete.py

import zlib
import zmq
import simplejson
import sys, os, datetime, time
import signal
from tradedangerous import tradedb, tradeenv

"""
 "  Configuration
"""
__reportDir             = '/tmp/eddnReports'
__relayEDDN             = 'tcp://eddn.edcd.io:9500'
__timeoutEDDN           = 600000

# Set False to listen to production stream
__debugEDDN             = False

# Set to False if you do not want verbose logging
#__logVerboseFile        = os.path.dirname(__file__) + '/Logs_Verbose_EDDN_%DATE%.htm'
__logVerboseFile        = False

# A sample list of authorised softwares
__authorisedSoftwares   = [
    "E:D Market Connector [Windows]",
    "E:D Market Connector [Linux]",
    "EDDiscovery",
    "EDDI",
    "EDCE",
    "ED-TD.SPACE",
    "EliteOCR",
    "Maddavo's Market Share",
    "RegulatedNoise",
    "RegulatedNoise__DJ"
]

# Used this to excludes yourself for example has you don't want to handle your own messages ^^
__excludedSoftwares     = [
    'My Awesome Market Uploader'
]

"""
 " Global variables
"""
exitGracefully = False

"""
 "  Start
"""
def signal_handler(sig, frame):
    print('SIGINT capturede, exiting gracefully.')
    global exitGracefully
    exitGracefully = True

def date(__format):
    d = datetime.datetime.utcnow()
    return d.strftime(__format)

def dumpJson(filename, json):
    echoFile(filename, simplejson.dumps(json, indent=2))

__oldTime = False
def echoLog(__str):
    global __oldTime, __logVerboseFile

    if __logVerboseFile != False:
        __logVerboseFileParsed = __logVerboseFile.replace('%DATE%', str(date('%Y-%m-%d')))

    if __logVerboseFile != False and not os.path.exists(__logVerboseFileParsed):
        f = open(__logVerboseFileParsed, 'w')
        f.write('<style type="text/css">html { white-space: pre; font-family: Courier New,Courier,Lucida Sans Typewriter,Lucida Typewriter,monospace; }</style>')
        f.close()

    if (__oldTime == False) or (__oldTime != date('%H:%M:%S')):
        __oldTime = date('%H:%M:%S')
        __str = str(__oldTime)  + ' | ' + str(__str)
    else:
        __str = '        '  + ' | ' + str(__str)

    print (__str)
    sys.stdout.flush()

    if __logVerboseFile != False:
        f = open(__logVerboseFileParsed, 'a')
        f.write(__str + '\n')
        f.close()

def echoFile(filename, __str):
    f = open(filename, 'a')
    f.write(__str + '\n')
    f.close()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    echoLog('Starting EDDN Subscriber')
    echoLog('')

    # From td.py in EDMC git
    # These are specific to Trade Dangerous, so don't move to edmc_data.py
    demandbracketmap = {0: '?',
                        1: 'L',
                        2: 'M',
                        3: 'H'}
    stockbracketmap = {0: '-',
                    1: 'L',
                    2: 'M',
                    3: 'H'}
    
    # Get the TradeDB object only once per runtime
    tdenv = tradeenv.TradeEnv(quiet=1)
    tdb = tradedb.TradeDB(tdenv)

    context     = zmq.Context()
    subscriber  = context.socket(zmq.SUB)

    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)

    def parseMessageCommodity(message):
        if len(message['commodities']) <= 0:
            return

        def exportCommodityToPricesFile(message):
            os.makedirs(__reportDir, exist_ok = True)
            filename = __reportDir + "/eddnReport_" + date('%Y-%m-%d-{:05d}.prices')
            cnt = 1
            while os.path.isfile(filename.format(cnt)):
                cnt = cnt + 1
            filename = filename.format(cnt)
            try:
                timestamp = datetime.datetime.strftime(datetime.datetime.strptime(message['timestamp'], '%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                timestamp = date('%Y-%m-%d %H:%M:%S')
            
            echoFile(filename, "#! trade import -")
            echoFile(filename, "# Created by EDDN client")
            echoFile(filename, "# Received on  " + timestamp)
            echoFile(filename, "")
            echoFile(filename, "#    <item name>             <sellCR> <buyCR>   <demand>   <stock>  <timestamp>")
            echoFile(filename, "@ " + message['systemName'] + " / " + message['stationName'])
            
            for commodity in message['commodities']:
                name = ''.join(e.lower() for e in commodity['name'] if e.isalnum()).lower()
                demandBracket=commodity['demandBracket']
                stockBracket=commodity['stockBracket']
                if demandBracket == '':
                    demandBracket=0
                if stockBracket == '':
                    stockBracket=0
                echoFile(filename, 
                    f"      {name:<23}"
                    f" {int(commodity['sellPrice']):7d}"
                    f" {int(commodity['buyPrice']):7d}"
                    f" {int(commodity['demand']) if demandBracket else '' :9}"
                    f"{demandbracketmap[demandBracket]:1}"
                    f" {int(commodity['stock']) if stockBracket else '':8}"
                    f"{stockbracketmap[stockBracket]:1}"
                    f"  {timestamp}"
                    )
            echoFile(filename, '')

        def importCommodityToTradeDB(message, station):
            items=[]
            try:
                timestamp = datetime.datetime.strftime(datetime.datetime.strptime(message['timestamp'], '%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                timestamp = date('%Y-%m-%d %H:%M:%S')

            for commodity in message['commodities']:
                itemName = commodity['name']
                try:
                    item = tdb.lookupItem(itemName, createNonExisting=True)
                except (LookupError, tradedb.AmbiguityError) as err:
                    echoLog("Adding {} failed: {}".format(itemName, err.__str__()))
                    continue
                
                items.append((
                    station.ID, item.ID, timestamp,
                    commodity['buyPrice'], commodity['demand'], commodity['demandBracket'] or -1,
                    commodity['sellPrice'], commodity['stock'], commodity['stockBracket'] or -1
            ))
            
            tdb.getDB().executemany("""
                    INSERT OR REPLACE INTO StationItem (
                        station_id, item_id, modified,
                        demand_price, demand_units, demand_level,
                        supply_price, supply_units, supply_level
                    ) VALUES (
                        ?, ?, IFNULL(?, CURRENT_TIMESTAMP),
                        ?, ?, ?,
                        ?, ?, ?
                    )
                """, items)
            tdb.getDB().commit()
        
        try:
            station = tdb.lookupStation(message['stationName'], message['systemName'])
            importCommodityToTradeDB(message, station)
            echoLog('- Commodities updated for: ' + message['systemName'] + " / " + message['stationName'])
        except LookupError:
            exportCommodityToPricesFile(message)
            echoLog('- Commodities exported for: ' + message['systemName'] + " / " + message['stationName'])

    dockedDumpCount = 0
    def parseMessageDocked(message):
        # Docked message, containing information about the station
        # Report system and station if the station contains a market
        if not message.get('StationServices', None):
            return
        if not "commodities" in message['StationServices']:
            return

        systemName = message['StarSystem']
        systemPos = message['StarPos']
        stationName = message['StationName']
        stationType = message["StationType"].lower()
        lsFromStar = int(message["DistFromStarLS"] + 0.5)
        planetary = "Y" if stationType in ("surfacestation","craterport","crateroutpost",) else "N"
        maxPadSize = "M" if stationType.startswith("outpost") else "L"
        fleet = "Y" if stationType == "fleetcarrier" else "N"
        odyssey = "Y" if stationType == "onfootsettlement" else "N"

        stnServices = [x.lower() for x in message.get('StationServices', None)]
        def getYNfromService(obj, key):
            return "Y" if key in obj else "N"
        blackmarket = getYNfromService(stnServices, 'blackmarket')
        market = getYNfromService(stnServices, 'commodities')
        shipyard = getYNfromService(stnServices, 'shipyard')
        outfitting = getYNfromService(stnServices, 'outfitting')
        rearm = getYNfromService(stnServices, 'rearm')
        refuel = getYNfromService(stnServices, 'refuel')
        repair = getYNfromService(stnServices, 'repair')

        try:
            timestamp = datetime.datetime.strftime(datetime.datetime.strptime(message['timestamp'], '%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            timestamp = date('%Y-%m-%d %H:%M:%S')
        
        sysNew=sysUpdate=False
        system = tdb.lookupSystem(systemName, exactOnly=True)
        if system:
            sysUpdate = tdb.updateLocalSystem(system=system,name=systemName,x=systemPos[0],y=systemPos[1],z=systemPos[2])
        else:
            tdb.addLocalSystem(name=systemName,x=systemPos[0],y=systemPos[1],z=systemPos[2])
            system = tdb.lookupSystem(systemName, exactOnly=True)
            sysNew=True
        
        statNew=statUpdate=False
        station = tdb.lookupStation(stationName, system, exactOnly=True)
        if station:
            statUpdate = tdb.updateLocalStation(
                station=station,
                lsFromStar=None,
                market=market,
                blackMarket=blackmarket,
                shipyard=shipyard,
                maxPadSize=maxPadSize,
                outfitting=outfitting,
                rearm=rearm,
                refuel=refuel,
                repair=repair,
                planetary=planetary,
                fleet=fleet,
                odyssey=odyssey
            )
        else:
            tdb.addLocalStation(
                system=system,name=stationName,lsFromStar=lsFromStar,
                market=market,
                blackMarket=blackmarket,
                shipyard=shipyard,
                maxPadSize=maxPadSize,
                outfitting=outfitting,
                rearm=rearm,
                refuel=refuel,
                repair=repair,
                planetary=planetary,
                fleet=fleet,
                odyssey=odyssey,
                modified=timestamp,
                commit=True
            )
            statNew=True
        
        if sysNew or statNew:
            echoLog("Created {}{} / {}{}.".format(
                systemName,
                " (new)" if sysNew else "",
                stationName,
                " (new)" if statNew else ""
                ))
        if sysUpdate or statUpdate:
            echoLog("Updated {} / {}.".format(systemName, stationName))

    global exitGracefully
    while not exitGracefully:
        try:
            subscriber.connect(__relayEDDN)
            echoLog('Connect to ' + __relayEDDN)
            echoLog('')
            echoLog('')

            while not exitGracefully:
                __message   = subscriber.recv()

                if __message == False:
                    subscriber.disconnect(__relayEDDN)
                    echoLog('Disconnect from ' + __relayEDDN)
                    echoLog('')
                    echoLog('')
                    break

                #echoLog('Got a message')

                __message   = zlib.decompress(__message)
                if __message == False:
                    echoLog('Failed to decompress message')

                __json      = simplejson.loads(__message)
                if __json == False:
                    echoLog('Failed to parse message as json')
                
                # Check authorisation and exclusion
                if not __json['header']['softwareName'] in __authorisedSoftwares:
                    continue
                if __json['header']['softwareName'] in __excludedSoftwares:
                    continue

                # Only process messages from the Odyssey version of the game.
                # Ignore legacay information
                if not __json['message'].get('odyssey', False):
                    continue

                # Handle commodity v3
                if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/commodity/3' + ('/test' if (__debugEDDN == True) else ''):
                    parseMessageCommodity(__json['message'])
                
                if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/journal/1' + ('/test' if (__debugEDDN == True) else ''):
                    if __json['message']['event'] == 'Docked':
                        parseMessageDocked(__json['message'])

                else:
                  # Schema not implemented
                  pass
                
                #else:
                #    echoLog('Unknown schema: ' + __json['$schemaRef']);


        except zmq.ZMQError as e:
            echoLog('')
            echoLog('ZMQSocketException: ' + str(e))
            subscriber.disconnect(__relayEDDN)
            echoLog('Disconnect from ' + __relayEDDN)
            echoLog('')
            time.sleep(5)



if __name__ == '__main__':
    main()
