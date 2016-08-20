#!/bin/bash

DATAFOLDER=/home/qy/sandbox_20160526_185949/

FILES="20160526 20160527"

for RAWCASHMDI in $FILES
do
    ./target/universal/stage/bin/dataprocconvtools $DATAFOLDER/$RAWCASHMDI"_ori.csv"  $DATAFOLDER/$RAWCASHMDI"_bbot"     cashmdi_bbottickbinary
    ./target/universal/stage/bin/dataprocconvtools $DATAFOLDER/$RAWCASHMDI"_bbot" $DATAFOLDER/$RAWCASHMDI"_restored.csv" bbottickbinary_cashmdi
done

ls -lh $DATAFOLDER/*
ls -l $DATAFOLDER/*
