#!/bin/bash

BIN="java -jar DataProcConvTools-assembly-1.0-SNAPSHOT.jar"

INPUTFOLDER1=output3_bars_300
OUTPUTFOLDER1=output_mdi

##################################################
# convert to bars
##################################################
find $INPUTFOLDER1/ -type f -name "2*csv" > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    stouch ./$OUTPUTFOLDER1/$line
    $BIN "$line" "$OUTPUTFOLDER1/$line" cashohlcfeed_cashmdi
done < file_list

