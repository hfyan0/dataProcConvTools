#!/bin/bash

DATAFOLDER=/mnt/d/data/ib_datacapture/
FILES="$1"

for YYYYMMDD in $FILES
do
    ./target/universal/stage/bin/dataprocconvtools $DATAFOLDER/$YYYYMMDD"_mdfdori.csv"  $DATAFOLDER/$YYYYMMDD"_bbot" cashmdi_bbottickbinary
    ./target/universal/stage/bin/dataprocconvtools $DATAFOLDER/$YYYYMMDD"_bbot" $DATAFOLDER/$YYYYMMDD"_restored.csv" bbottickbinary_cashmdi
done

ls -lh $DATAFOLDER/*
ls -l $DATAFOLDER/*
