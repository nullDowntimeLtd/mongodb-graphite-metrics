#!/usr/bin/env python2.7

import pymongo, argparse, commands
from pymongo import Connection
import time, sys
from datetime import datetime
from datetime import timedelta
from socket import socket
#from bson import json_util
#import json

thisHost = commands.getoutput('hostname')

parser = argparse.ArgumentParser(description='Creates graphite metrics for a single mongodb instance from administation commands.')
parser.add_argument('-host', default=thisHost,
                   help='host name of mongodb to create metrics from.')
parser.add_argument('-port', default=27017,
                   help='port of mongodb to create metrics from.')
parser.add_argument('-prefix', default='DEV',
                   help='prefix for all metrics.')
parser.add_argument('-node', required=True,
                   help='node name the metrics should appear under.')
parser.add_argument('-user', required=True,
                   help='mongodb username')
parser.add_argument('-passwd', required=True,
                   help='mongodb password')
parser.add_argument('-graphiteHost', required=True,
                   help='host name for graphite server.')
parser.add_argument('-graphitePort', required=True,
                   help='port garphite is listening on.')
parser.add_argument('-replica', required=False, default=None,
                   help='mongodb replicaset.')
args = parser.parse_args()

carbonHost = args.graphiteHost
carbonPort = int(args.graphitePort)
host = args.host
port = args.port
user = args.user
passwd = args.passwd

try:
  connection = Connection(host, port, slave_okay=True, network_timeout=10)
  if user and passwd:
    db = connection["admin"]
    if not db.authenticate(user, passwd): sys.exit("Username/Password incorrect")
except Exception, e:
  print str(e)
  sys.exit(1)

metricName = args.prefix+'.mongodb.'+args.node+'.'

def uploadToCarbon(metrics):
  now = int( time.time() )
  lines = []

  for name, value in metrics.iteritems() :
    lines.append(metricName+name+' %s %d' % (value, now))

  message = '\n'.join(lines) + '\n'

  sock = socket()
  try:
    sock.connect( (carbonHost,carbonPort) )
  except:
    print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server': carbonHost, 'port': carbonPort}
    sys.exit(1)
  #print message
  sock.sendall(message)

def calculateLagTimes(replStatus, primaryDate):
  lags = dict()
  for hostState in replStatus['members'] :
    lag = primaryDate - hostState['optimeDate']
    hostName = hostState['name'].lower().split('.')[0]
    lags[hostName+".lag_seconds"] = '%.0f' % ((lag.microseconds + (lag.seconds + lag.days * 24 * 3600) * 10**6) / 10**6)
  return lags

def gatherReplicationMetrics():
  replicaMetrics = dict()
  replStatus = connection.admin.command("replSetGetStatus");

  for hostState in replStatus['members'] :
    if hostState['stateStr'] == 'PRIMARY' and hostState['name'].lower().startswith(host) :
      lags = calculateLagTimes(replStatus, hostState['optimeDate'])
      replicaMetrics.update(lags)
    if hostState['name'].lower().startswith(host) :
      thisHostsState = hostState
      replicaMetrics['state'] = thisHostsState['state']

  return replicaMetrics

def gatherServerStatusMetrics():
  serverMetrics = dict()
  serverStatus = connection.admin.command("serverStatus");

  #print json.dumps(serverStatus, default=json_util.default, sort_keys=True, indent=2, separators=(',', ': '))

  serverMetrics['lock.totaltime'] = '%.5f' % serverStatus['globalLock']['totalTime']
  serverMetrics['lock.locktime'] = '%.5f' % serverStatus['globalLock']['lockTime']
  serverMetrics['lock.queue.total'] = serverStatus['globalLock']['currentQueue']['total']
  serverMetrics['lock.queue.readers'] = serverStatus['globalLock']['currentQueue']['readers']
  serverMetrics['lock.queue.writers'] = serverStatus['globalLock']['currentQueue']['writers']
  serverMetrics['lock.activeclients.total'] = serverStatus['globalLock']['activeClients']['total']
  serverMetrics['lock.activeclients.readers'] = serverStatus['globalLock']['activeClients']['readers']
  serverMetrics['lock.activeclients.writers'] = serverStatus['globalLock']['activeClients']['writers']

  serverMetrics['connections.current'] = serverStatus['connections']['current']
  serverMetrics['connections.available'] = serverStatus['connections']['available']
  serverMetrics['connections.totalCreated'] = serverStatus['connections']['totalCreated']

  serverMetrics['indexes.accesses'] = serverStatus['indexCounters']['accesses']
  serverMetrics['indexes.hits'] = serverStatus['indexCounters']['hits']
  serverMetrics['indexes.misses'] = serverStatus['indexCounters']['misses']
  serverMetrics['indexes.missRatio'] = '%.5f' % serverStatus['indexCounters']['missRatio']
  serverMetrics['indexes.resets'] = serverStatus['indexCounters']['resets']

  serverMetrics['cursors.open'] = serverStatus['metrics']['cursor']['open']['total']
  serverMetrics['cursors.timedOut'] = serverStatus['metrics']['cursor']['timedOut']

  serverMetrics['mem.residentMb'] = serverStatus['mem']['resident']
  serverMetrics['mem.virtualMb'] = serverStatus['mem']['virtual']
  serverMetrics['mem.mapped'] = serverStatus['mem']['mapped']
  serverMetrics['mem.page_faults'] = serverStatus['extra_info']['page_faults']
  serverMetrics['mem.heap_usage_bytes'] = serverStatus['extra_info']['heap_usage_bytes']

  serverMetrics['net.bytesIn'] = serverStatus['network']['bytesIn']
  serverMetrics['net.bytesOut'] = serverStatus['network']['bytesOut']
  serverMetrics['net.numRequests'] = serverStatus['network']['numRequests']

  serverMetrics['asserts.warnings'] = serverStatus['asserts']['warning']
  serverMetrics['asserts.errors'] = serverStatus['asserts']['msg']

  return serverMetrics


metrics = dict()
metrics.update(gatherReplicationMetrics())
metrics.update(gatherServerStatusMetrics())

uploadToCarbon(metrics)
