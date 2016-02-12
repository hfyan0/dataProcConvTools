#!/bin/bash

##################################################
find fromWinky/ -type f -name "tt_*" > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    DATESTR=$(echo $line | sed -e 's/^.*_20/20/')
    stouch ./output/$line
    java -jar DataProcConvTools-assembly-1.0-SNAPSHOT.jar hkexfmt1 "$line" "output/$line" 300 $DATESTR 090000 160000
done < file_list


##################################################
find ./output -type f > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    stouch ./sorted/$line
    cat $line | sort -s > ./sorted/$line
done < file_list


##################################################
find fromWinky/ -type f | sed -e 's/^.*_//' | sort | uniq > date_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "date: $line"
    stouch ./sorted2/$line".csv"
    find ./sorted/ -type f -name "*$line" | xargs cat | sort -s > ./sorted2/$line".csv"
done < date_list
