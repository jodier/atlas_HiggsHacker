#!/bin/sh

#############################################################################

DIR=`pwd`

cd $3

#############################################################################

pathena --inDS $1 --outDS $2 $4 uD3PD.py

#############################################################################

cd $DIR

#############################################################################

