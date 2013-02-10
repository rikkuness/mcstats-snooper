#!/bin/bash
#######################################
#
# Mojang Snooper Data to MySQL (wrapper)
# author: Darrian Hitch
# date: 08/02/13
#
#######################################

PARSER="/home/rikkuness/Code/MojangData/parser.py"
DATA_DIR="/dev/shm/server_data"

for DATA_FILE in $(ls $DATA_DIR/server*.snoopdata.gz)
do
	printf "Loading $DATA_FILE into parser...\n"
	zcat $DATA_FILE | $PARSER
	printf "\n"
done