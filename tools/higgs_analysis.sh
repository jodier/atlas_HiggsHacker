#!/bin/sh

#############################################################################

DIR=`pwd`

HOME=/afs/cern.ch/user/j/jodier

cd /$HOME/testarea/16.6.X/higgs_analysis

#############################################################################

cp core/include/data_uD3PD_THiggsD3PD.h core/include/THiggsD3PD.h

SRCS=\
config.txt\
core/main.cc,\
core/core.cc,\
core/config.cc,\
core/loader.cc,\
core/utils.cc,\
core/include/core.h,\
core/include/THiggsD3PD.h,\
core/include/athena/egammaPIDdefs.h,\
tools/checkOQ.h,\
tools/egammaSFclass.h,\
tools/EnergyRescaler.h,\
tools/IsEMPlusPlusDefs.h,\
tools/CaloIsoCorrection.h,\
tools/TPileupReweighting.h,\
tools/checkOQ.cc,\
tools/egammaSFclass.cc,\
tools/EnergyRescaler.cc,\
tools/IsEMPlusPlusDefs.cc,\
tools/CaloIsoCorrection.cc,\
tools/TPileupReweighting.cc,\
analysis/higgs_analysis/main.h,\
analysis/higgs_analysis/main.cc,\
analysis/higgs_analysis/utils.h,\
analysis/higgs_analysis/utils.cc,\
analysis/higgs_analysis/triggers.cc,\
analysis/higgs_analysis/Z_analysis.cc,\
analysis/higgs_analysis/H_analysis.cc,\
analysis/higgs_analysis/isEMPlusPlus.cc

prun --exec "higgs_analysis --grid=prun --enable-ER -o output.root %IN" --bexec "make" \
--athenaTag=16.6.6 \
--inDS $1 \
--outDS $2 \
--extFile $SRCS \
--outputs output.root \
--excludedSite ANALY_CERN_XROOTD

#############################################################################

cd $DIR

#############################################################################

