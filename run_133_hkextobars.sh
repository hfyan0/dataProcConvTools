#!/bin/bash

BIN="java -jar DataProcConvTools-assembly-1.0-SNAPSHOT.jar"
BARINTERVALINSEC=300
#BARINTERVALINSEC=3600
OUTPUTFOLDER1=output1_bars_$BARINTERVALINSEC
OUTPUTFOLDER2=output2_bars_$BARINTERVALINSEC
OUTPUTFOLDER3=output3_bars_$BARINTERVALINSEC

##################################################
# convert to bars
##################################################
find fromWinky/ -type f -name "tt_*" > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    DATESTR=$(echo $line | sed -e 's/^.*_20/20/')
    stouch ./$OUTPUTFOLDER1/$line
    $BIN "$line" "$OUTPUTFOLDER1/$line" hkex1_cashohlcfeed $BARINTERVALINSEC $DATESTR 090000 160000
done < file_list


##################################################
# by original file
##################################################
find ./$OUTPUTFOLDER1 -type f > file_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "file: $line"
    stouch ./$OUTPUTFOLDER2/$line
    cat $line | sort -s > ./$OUTPUTFOLDER2/$line
done < file_list


##################################################
# sort by date
##################################################
find fromWinky/ -type f | sed -e 's/^.*_//' | grep ^20 | sort | uniq > date_list

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "date: $line"
    stouch ./$OUTPUTFOLDER3/$line".csv"
    find ./$OUTPUTFOLDER2/ -type f -name "*$line" | xargs cat | sort -s > ./$OUTPUTFOLDER3/$line".csv"
done < date_list
