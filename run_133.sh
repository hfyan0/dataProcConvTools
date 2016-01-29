#!/bin/bash

find fromWinky/ -type f -name "tt_*" > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    DATESTR=$(echo $line | sed -e 's/^.*_20/20/')
    java -jar DataProcConvTools-assembly-1.0-SNAPSHOT.jar hkexfmt1 "$line" "output/$line" 300 $DATESTR 090000 160000
done < file_list
