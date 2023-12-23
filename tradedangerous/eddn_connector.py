#!/usr/bin/env python3
# Based on https://github.com/EDCD/EDDN/blob/master/examples/Python%203.4/Client_Complete.py

import zlib
import zmq
import simplejson
import sys, os, time
from datetime import datetime, timedelta
import signal
from tradedangerous.tradeenv import TradeEnv
from tradedangerous.tradedb import TradeDB, AmbiguityError

"""
 "  Configuration
"""
__reportDir             = '/tmp/eddnReports'
__relayEDDN             = 'tcp://eddn.edcd.io:9500'
__timeoutEDDN           = 600000

# Set False to listen to production stream
__debugEDDN             = False

# Set to False if you do not want verbose logging
#__logVerboseFile        = os.path.dirname(__file__) + '/EDDN_Connector-%DATE%.log'
__logVerboseFile        = False

# Error log file
__errorLogFile          = __reportDir + "/error.log"

# Check https://eddn.edcd.io/ for statistics
# A sample list of authorised softwares
__authorisedSoftwares   = [
    "E:D Market Connector [Linux]",
    "E:D Market Connector [Mac]",
    "E:D Market Connector [Windows]",
    "EDDiscovery",
    "EDO Materials Helper",
    "EDDI",
    "EDSM",
    "EDDLite",
    "Journal Limpet"
]

# Used this to excludes yourself for example as you don't want to handle your own messages
__excludedSoftwares     = [
    'GameGlass',
]

"""
 " Global variables
"""
exitGracefully = False

"""
 "  Start
"""
def signal_handler(sig, frame):
    print('SIGINT/SIGTERM capturede, exiting gracefully.')
    global exitGracefully
    exitGracefully = True

def date(__format):
    d = datetime.utcnow()
    return d.strftime(__format)

def getTimeStamp(string):
    stdFormat = '%Y-%m-%dT%H:%M:%S%z' # ISO 8601
    for fmt in (
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%d %H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S.%fZ'
        ):
        try:
            return datetime.strptime(string, fmt).strftime(stdFormat)
        except ValueError:
            pass
    echoLog("#ERROR: Timestamp not recognised: %s" % string)
    return date(stdFormat)

def dumpJson(filename, json):
    if os.path.exists(filename):
        os.unlink(filename)
    echoFile(filename, simplejson.dumps(json, indent=2))

__oldTime = False
def echoLog(__str):
    global __oldTime, __logVerboseFile

    if __logVerboseFile != False:
        __logVerboseFileParsed = __logVerboseFile.replace('%DATE%', str(date('%Y-%m-%d')))

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

__lastCleanup = None
def cleanupDb(tdb: TradeDB):
    global __lastCleanup
    # Only cleanup once a day
    if __lastCleanup and __lastCleanup == date("%Y-%m-%d"):
        return
    __lastCleanup = date("%Y-%m-%d")
    echoLog("Cleanup DB.")
    stdFormat = '%Y-%m-%dT%H:%M:%S%z' # ISO 8601
    timestamp = datetime.utcnow() - timedelta(days = 14)
    command = "DELETE FROM StationItem WHERE modified < '%s'" % timestamp.strftime(stdFormat)
    tdb.getDB().execute(command)
    tdb.getDB().commit()
    tdb.query("VACUUM")
    tdb.getDB().commit()
    tdb.close() # Close to cleanup everything
    tdb.load() # Reload database

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    eddnConnectorQuiet=os.environ.get('EDDN_CONNECTOR_QUIET') or False
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
    tdenv = TradeEnv(quiet=1)
    tdb = TradeDB(tdenv)

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
            timestamp = getTimeStamp(message['timestamp'])
            
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
            timestamp = getTimeStamp(message['timestamp'])

            for commodity in message['commodities']:
                itemName = commodity['name']
                try:
                    item = tdb.lookupItem(itemName, createNonExisting=True)
                except (LookupError, AmbiguityError) as err:
                    echoLog("Adding {} failed: {}".format(itemName, err.__str__()))
                    continue
                
                if commodity['demandBracket'] == "": demandBracket = -1
                else: demandBracket = commodity['demandBracket']
                if commodity['stockBracket'] == "": stockBracket = -1
                else: stockBracket = commodity['stockBracket']
                items.append((
                    station.ID, item.ID, timestamp,
                    commodity['sellPrice'], commodity['demand'], demandBracket,
                    commodity['buyPrice'], commodity['stock'], stockBracket
            ))
            
            # Remove old entries
            tdb.getDB().execute("DELETE FROM StationItem WHERE station_id = {:d}".format(station.ID))
            # Add the items from the log
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
            system = tdb.lookupSystem(message['systemName'], exactOnly=True)
            station = tdb.lookupStation(message['stationName'], system, exactOnly=True)
            importCommodityToTradeDB(message, station)
            if not eddnConnectorQuiet:
                echoLog('- Updated prices for: ' + message['systemName'] + " / " + message['stationName'])
        except LookupError:
            #exportCommodityToPricesFile(message)
            #echoLog('- Exported prices for: ' + message['systemName'] + " / " + message['stationName'])
            if not eddnConnectorQuiet:
                echoLog('- Ignore prices for unknown station: ' + message['systemName'] + " / " + message['stationName'])

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
        if stationType == "fleetcarrier":
            # Do not parse fleet carrier messages.
            return
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

        timestamp = getTimeStamp(message['timestamp'])
        
        sysNew=sysUpdate=False
        try:
            system = tdb.lookupSystem(systemName, exactOnly=True)
            sysUpdate = tdb.updateLocalSystem(system=system,prettyName=systemName,x=systemPos[0],y=systemPos[1],z=systemPos[2])
        except LookupError:
            tdb.addLocalSystem(prettyName=systemName,x=systemPos[0],y=systemPos[1],z=systemPos[2])
            system = tdb.lookupSystem(systemName, exactOnly=True)
            sysNew=True
        
        statNew=statUpdate=False
        try:
            station = tdb.lookupStation(stationName, system, exactOnly=True)
            statUpdate = tdb.updateLocalStation(station=station, lsFromStar=None, market=market,
                blackMarket=blackmarket, shipyard=shipyard, maxPadSize=maxPadSize, outfitting=outfitting,
                rearm=rearm, refuel=refuel, repair=repair, planetary=planetary, fleet=fleet, odyssey=odyssey
            )
        except LookupError:
            tdb.addLocalStation( system=system, prettyName=stationName, lsFromStar=lsFromStar, market=market,
                blackMarket=blackmarket, shipyard=shipyard, maxPadSize=maxPadSize, outfitting=outfitting,
                rearm=rearm, refuel=refuel, repair=repair, planetary=planetary, fleet=fleet,
                odyssey=odyssey, modified=timestamp, commit=True
            )
            station = tdb.lookupStation(name=stationName, system=system, exactOnly=True)
            statNew=True
        
        if (sysNew or statNew) and not eddnConnectorQuiet:
            echoLog("Created {}{} / {}{}.".format(
                system.name(),
                " (new)" if sysNew else "",
                station.name(),
                " (new)" if statNew else ""
                ))
        if (sysUpdate or statUpdate) and not eddnConnectorQuiet:
            echoLog("Updated {} / {}.".format(system.name(), station.name()))

    global exitGracefully
    while not exitGracefully:
        try:
            subscriber.connect(__relayEDDN)
            echoLog('Connect to ' + __relayEDDN)
            echoLog('')
            echoLog('')

            while not exitGracefully:
                cleanupDb(tdb) # Cleanup regularily
                __message   = subscriber.recv()

                if __message == False:
                    subscriber.disconnect(__relayEDDN)
                    echoLog('Disconnect from ' + __relayEDDN)
                    echoLog('')
                    echoLog('')
                    break

                __message   = zlib.decompress(__message)
                if __message == False:
                    echoLog('Failed to decompress message')

                __json      = simplejson.loads(__message)
                if __json == False:
                    echoLog('Failed to parse message as json')
                
                # Check authorisation and exclusion
                if __json['header']['softwareName'] in __excludedSoftwares:
                    continue
                if not __json['header']['softwareName'] in __authorisedSoftwares:
                    continue

                # Only process messages from the Odyssey version of the game.
                # Ignore legacay information
                if not __json['message'].get('odyssey', False):
                    continue

                try:
                    # Handle commodity v3
                    if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/commodity/3' + ('/test' if (__debugEDDN == True) else ''):
                        parseMessageCommodity(__json['message'])
                    
                    # Handle journal entries ("docked" contain required system and station information)
                    if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/journal/1' + ('/test' if (__debugEDDN == True) else ''):
                        if __json['message']['event'] == 'Docked':
                            parseMessageDocked(__json['message'])
                
                except Exception as error:
                    echoLog(error.__str__())
                    echoFile(__errorLogFile, error.__str__())

        except zmq.ZMQError as e:
            echoLog('')
            echoLog('ZMQSocketException: ' + str(e))
            subscriber.disconnect(__relayEDDN)
            echoLog('Disconnect from ' + __relayEDDN)
            echoLog('')
            time.sleep(5)



if __name__ == '__main__':
    main()
