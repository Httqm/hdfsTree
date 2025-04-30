#!/usr/bin/env bash

APP_PLATEFORME='DEV'
app_plateforme=${APP_PLATEFORME,,}


treeBaseDir="/run/user/$UID/HDFSTREE.SH"
# where we'll store our HDFS tree on the local filesystem


######################################### HDFS ######################################################
hdfsRoot="$treeBaseDir/HDFS_ROOT"
# the directory representing the '/' of the HDFS storage


hdfsWorkDir="/$APP_PLATEFORME/data_lake"
# where the actual 'tree' structure ("datasource/dataset/year=/month=/day=/data") starts within the HDFS storage
# the leading '/' corresponds to "$hdfsRoot"


# the values used to build the local HDFS tree (by running 'hdfsTree.sh create')
# format: 'dataSource dataSet depth'
# - dataSource:	explicit
# - dataSet:	explicit
# - depth:		this is the level, in the tree structure, where the datafiles are found:
#		dataSource/dataSet/year=/month=/day=/data			<== "$depth": 'day'
#		dataSource/dataSet/year=/month=/day=/hour=/data		<== "$depth": 'hour'
hdfsTreeData='ABACUS DEALS_DELTA hour
ABACUS IMPORTED_REQUESTS_DELTA hour
ALCYONE CHECK_AGGREGATED_DATA_DELTA day
ALCYONE DIM_ACCOUNT_H1 day
FEDERATED_INVENTORY AGING day
FEDERATED_INVENTORY AGREEMENT day
MM2 2023 day'


nbDatafilePerDir=2
datafileSize='1M'


######################################### S3 ########################################################
s3Root="$treeBaseDir/S3_ROOT"
s3Buckets="edh-${app_plateforme}-raw-data edh-${app_plateforme}-backup-data"
