#!/usr/bin/python
# -*- coding: utf-8 -*-
#######################################
#
# Mojang Snooper Data to MySQL (server)
# author: Darrian Hitch (@rikkuness)
# date: 09/02/13
#
#######################################
import simplejson as json				# JSON parser library
import MySQLdb as mysql					# MySQL connect library
import fileinput						# For accepting stdin via pipe
import time								# Timing component
from sys import stdout, exit			# System functions
from warnings import filterwarnings		# Filter MySQL warnings

# Variables
i = 0					# Loop counter
t = 0					# Transaction counter
data_list = []			# Data entry list
bulk_limit = 10000		# Data list length limit
default_int = "0"		# Default return for null int
default_str = ""		# Default return for null string
db_host = "localhost"	# MySQL server
db_user = "snooper"		# MySQL user account
db_pass = "password"	# MySQL user account password
db_name = "snooper"		# MySQL database name
db_tbl = "server_data"	# MySQL table name

# Setup initial database connection
filterwarnings('ignore', category = mysql.Warning)
db = None
try:
	db = mysql.connect(db_host, db_user, db_pass, db_name)
except db.Error, e:
	print 'Database connection failed %d: %s' % (e.args[0],e.args[1])
	exit(1)

# Create the table if it's not already there
with db:
	this = db.cursor()
	this.execute("CREATE TABLE IF NOT EXISTS %(db_tbl)s(\
id INT PRIMARY KEY AUTO_INCREMENT, \
os_name VARCHAR(25), \
os_arch VARCHAR(10), \
os_version VARCHAR(50), \
cpu_cores TINYINT, \
mem_total BIGINT, \
mem_free BIGINT, \
mem_max BIGINT, \
java_version VARCHAR(20), \
server_brand VARCHAR(35), \
version VARCHAR(9), \
dedicated VARCHAR(5), \
gui_state VARCHAR(8), \
gui_supported VARCHAR(9), \
singleplayer VARCHAR(5), \
players_max INT, \
players_current SMALLINT, \
players_seen MEDIUMINT, \
uses_auth VARCHAR(5), \
worlds TINYINT, \
snooper_count INT, \
snooper_token VARCHAR(37)\
);" % locals())

# Base query to load data
qry = "INSERT INTO %(db_tbl)s(os_name, os_arch, os_version, cpu_cores, \
mem_total, mem_free, mem_max, java_version, server_brand, version, \
dedicated, gui_state, gui_supported, singleplayer, players_max, \
players_current, players_seen, uses_auth, worlds, snooper_count, \
snooper_token) VALUES " % locals()

# Start the timers
timer = time.time()
start = time.time()

# Begin accepting data from stdin
for entry in fileinput.input():

	# Decode JSON string from stdin
	record = json.JSONDecoder().decode(entry)

	# Operating environment
	os_name = record.get('os_name', default_str)
	os_arch = record.get('os_architecture', default_str)
	os_version = record.get('os_version', default_str)
	cpu_cores = record.get('cpu_cores', default_int)
	mem_total = record.get('memory_total', default_int)
	mem_free = record.get('memory_free', default_int)
	mem_max = record.get('memory_max', default_int)
	java_version = record.get('java_version', default_str)

	# Minecraft server information
	server_brand = record.get('server_brand', default_str)
	version = record.get('version', default_str)
	dedicated = record.get('dedicated', default_str)
	gui_state = record.get('gui_state', default_str)
	gui_supported = record.get('gui_supported', default_str)
	singleplayer = record.get('singleplayer', default_str)
	players_max = record.get('players_max', default_int)
	players_current = record.get('players_current', default_int)
	players_seen = record.get('players_seen', default_int)
	uses_auth = record.get('uses_auth', default_str)
	worlds = record.get('worlds', default_int)
	snooper_count = record.get('snooper_count', default_int)
	snooper_token = record.get('snooper_token', default_str)

	# Load the data string with vars
	data = '("%(os_name)s", "%(os_arch)s", "%(os_version)s", "%(cpu_cores)s", \
"%(mem_total)s", "%(mem_free)s", "%(mem_max)s", "%(java_version)s", \
"%(server_brand)s", "%(version)s", "%(dedicated)s", "%(gui_state)s", \
"%(gui_supported)s", "%(singleplayer)s", "%(players_max)s", \
"%(players_current)s", "%(players_seen)s", "%(uses_auth)s", "%(worlds)s", \
"%(snooper_count)s", "%(snooper_token)s")' % locals()

	# Append the data string to the list
	data_list.append(data)

	# If the data list is full then bulk insert the data
	if i > bulk_limit:

		# Create the data string to be used in the MySQL query
		bulk_data = ', '.join(map(str, data_list))

		# Execute MySQL bulk insert
		with db:
			this = db.cursor()
			this.execute(qry + bulk_data)

		# Write out transactions per second (tps) to stdout
		stdout.write("\r%s\ttps" % round(bulk_limit / (time.time() - timer)))
		stdout.flush()

		# Reset counters
		timer = time.time()
		data_list = []
		i = 0
	else:
		# Increase iteration counter
		i = i + 1

	# Increment transaction counter
	t = t + 1

# End of main data loop
stdout.write("\n")

# Dump up any leftovers to the database and close the connection
bulk_data = ', '.join(map(str, data_list))
with db:
	this = db.cursor()
	this.execute(qry + bulk_data)
db.close()

# Finishing off
print 'Processed %s entries in %s minutes!' % (t, (round(time.time() - start)) / 60)