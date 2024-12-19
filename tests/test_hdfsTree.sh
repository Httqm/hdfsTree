#!/usr/bin/env bash

setup_suite() {
	echo 'setup_suite()'
	source '../hdfsTree_config.sh'

	HDFS='hdfs'
	HADOOP='hadoop'

	[ -L "../$HDFS" -a -L "../$HADOOP" ] || {
		echo "Missing symlinks, please run 'cd ..; ./hdfsTree.sh catch' and retry"
		exit 1
		}

	which "$HDFS" 1>/dev/null || {
		# 'hdfs' commands are expected to be caught by '../hdfsTree.sh' + 'hdfs' symlink
		#  + "$PATH" update (see "catch" option / "showCatchCommands()" function in
		# '../hdfsTree.sh' for details)
		export PATH=$PATH:../
		}
	[ -d "$hdfsRoot/$hdfsWorkDir" ] && true || ../hdfsTree.sh create 'hdfs'

	which "$HADOOP" 1>/dev/null || export PATH=$PATH:../	# as above
	defaultDestinationS3Bucket='edh-dev-raw-data'	# TODO: hardcoded S3 bucket name
	[ -d "$s3Root/$defaultDestinationS3Bucket" ] && true || ../hdfsTree.sh create 's3'
	}


#teardown_suite() {
#	echo "Don't forget to delete the HDFS tree created for the tests."
#	}


test_hdfs_dfs_ls() {
	# real behaviour :
	# hdfs dfs -ls /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12
	#	Found 1 items
	#	-rw-rw-r--   3 adedhdlkuat gpedhdlkuat          0 2024-02-28 20:05 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12/<dataFile>
	pathToList="$hdfsWorkDir/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12"
	result=$($HDFS dfs -ls "$pathToList")
	assert_matches "^-rw.* $pathToList/[[:print:]]+$" "$result"
	}


test_hdfs_dfs_ls_-C() {
	# real behaviour :
	# hdfs dfs -ls -C /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12
	#	/UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12/<dataFile>
	pathToList="$hdfsWorkDir/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=12"
	result=$($HDFS dfs -ls -C "$pathToList")

	# checking individual result lines match the expected format
	while read resultLine; do
#		echo "'$resultLine'"
		assert_matches "^$pathToList/[[:print:]]+$" "$resultLine"
	done <<< "$result"

	# checking we effectively get "$nbDatafilePerDir" result lines
	assert_equals "$nbDatafilePerDir" $(wc -l <<< "$result")
	}


test_hdfs_dfs_ls_-R_depthHour() {
	# real behaviour :
	#	hdfs dfs -ls -R /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-02-28 20:05 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=00
	#		-rw-rw-r--   3 adedhdlkuat gpedhdlkuat          0 2024-02-28 20:05 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=00/<dataFile>
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-02-28 05:30 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=01
	#		-rw-rw-r--   3 adedhdlkuat gpedhdlkuat          0 2024-02-28 05:30 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=01/<dataFile>
	#		...
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-02-29 04:35 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=23
	#		-rw-rw-r--   3 adedhdlkuat gpedhdlkuat          0 2024-02-29 04:35 /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=23/<dataFile>
	#
	# actual behaviour :
	#		/UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28/hour=00		<== see note
	#		-rw-rw-r--   3 adedhdlkuat gpedhdlkuat          0 2024-02-28 20:05 /UAT/data_lake/ABACUS/IMPORTED_REQUES
	#	note: the path only is displayed, owner+group+permissions+size+... are ommitted
	pathToList="$hdfsWorkDir/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=02/day=28"
	nbHoursPerDay=24
	result=$($HDFS dfs -ls -R "$pathToList")

#	echo "$result"
	numberOfDirectoriesListed=$(grep -Ec "$pathToList/hour=[0-9]{2}$" <<< "$result")
	assert_equals "$nbHoursPerDay" "$numberOfDirectoriesListed"

#	echo "$result"
	numberOfDataFilesListed=$(grep -Ec "$pathToList/hour=[0-9]{2}/[[:print:]]+" <<< "$result")
	assert_equals $((nbHoursPerDay*nbDatafilePerDir)) "$numberOfDataFilesListed"
	}


test_hdfs_dfs_ls_-R_depthDay() {
	# real behaviour :
	#	hdfs dfs -ls -R /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-03-07 02:24 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=01
	#		-rw-r--r--   3 adedhdlkuat gpedhdlkuat      25291 2024-03-07 02:24 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=01/<dataFile>
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-03-08 02:49 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02
	#		-rw-r--r--   3 adedhdlkuat gpedhdlkuat      25302 2024-03-08 02:49 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02/<dataFile>
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-03-09 03:01 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=03
	#		...
	#		drwxrwxr-x   - adedhdlkuat gpedhdlkuat          0 2024-04-03 03:31 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=29
	#		-rw-r--r--   3 adedhdlkuat gpedhdlkuat      25347 2024-04-03 03:31 /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=29/<dataFile>

	pathToList="$hdfsWorkDir/FEDERATED_INVENTORY/AGING/year=2024/month=02"
	nbDaysInChosenMonth=29	# feb 2024
	result=$($HDFS dfs -ls -R "$pathToList")

#	echo "$result"
	numberOfDirectoriesListed=$(grep -Ec "$pathToList/day=[0-9]{2}$" <<< "$result")
	assert_equals "$nbDaysInChosenMonth" "$numberOfDirectoriesListed"

#	echo "$result"
	numberOfDataFilesListed=$(grep -Ec "$pathToList/day=[0-9]{2}/[[:print:]]+" <<< "$result")
	assert_equals $((nbDaysInChosenMonth*nbDatafilePerDir)) "$numberOfDataFilesListed"
	}


test_hdfs_dfs_count() {
	# real behaviour :
	#	hdfs dfs -count /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02
	#		25	48	607530	/UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02
	#
	#	fields : DIR_COUNT, FILE_COUNT, CONTENT_SIZE, PATHNAME

	pathToList="$hdfsWorkDir/FEDERATED_INVENTORY/AGING/year=2024/month=02"
	nbDaysInChosenMonth=29	# feb 2024
	result=$($HDFS dfs -count "$pathToList")

	expectedNbDirectories=$((nbDaysInChosenMonth+1))
	# counts all directories + the './'

	expectedNbFiles=$((nbDaysInChosenMonth*nbDatafilePerDir))

	assert_matches "^ +$expectedNbDirectories +$expectedNbFiles +[0-9]+ +$pathToList$" "$result"
	}


# Output of
#	hdfs dfs -count /UAT/data_lake/
# followed by
#	hdfs dfs -count /UAT/data_lake/*
# are not nicely aligned in columns. There _may_ be a way to change that,
# but not sure it's worth so far.
#test_hdfs_dfs_count_columns() {
#	echo
#	for pathToList in '/UAT/data_lake/' '/UAT/data_lake/*'; do
#		result=$($HDFS dfs -count "$pathToList")
##		echo "$result" | tr ' ' '.'
#		while IFS= read resultLine; do	# 'IFS=' hack is to keep leading spaces
#			echo "$resultLine" | tr ' ' '.'
#		done <<< "$result"
#	done
#	}


test_hdfs_dfs_count_asterisk() {
	# real behaviour:
	#	hdfs dfs -count '/UAT/data_lake/*'
	#	    1130         1847        19048647571 /UAT/data_lake/2IM
	#	       3            0                  0 /UAT/data_lake/44E
	#	    1093        28968      1571899761461 /UAT/data_lake/47O
	#	      18           15          150730273 /UAT/data_lake/50D
	#	       3            0                  0 /UAT/data_lake/56N_NOS_VENTE
	#	     ...
	#
	#	hdfs dfs -count '/UAT/data_lake/FEDERATED_INVENTORY/*'
	#	     509          960           14443092 /UAT/data_lake/FEDERATED_INVENTORY/AGING
	#	     212          392         2748463956 /UAT/data_lake/FEDERATED_INVENTORY/AGREEMENT
	#	      40           68         1824366270 /UAT/data_lake/FEDERATED_INVENTORY/BA_RECEIVES_CE
	#	     213          394        20343261697 /UAT/data_lake/FEDERATED_INVENTORY/BILLINGACCOUNT
	#	     ...

	pathToList="$hdfsWorkDir/"*
	result=$($HDFS dfs -count "$pathToList")

	# check I get a series of lines : "<nb_dirs> <nb_files> <size> <fielname>"
	while read resultLine; do
#		echo "$resultLine"
		assert_matches "^ *([0-9]+ +){3}$pathToList" "$resultLine"
	done <<< "$result"
	}


test_hdfs_dfs_du_directories() {
	# real behaviour:
	#	hdfs dfs -du /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02
	#		25362  /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=01
	#		25337  /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02
	#		25420  /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=03
	#		...
	#		25291  /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=29

	pathToList="$hdfsWorkDir/FEDERATED_INVENTORY/AGING/year=2024/month=02"
	result=$($HDFS dfs -du "$pathToList")

	# check I get a series of lines "<size>	<path>"
	while read resultLine; do
#		echo "$resultLine"
		assert_matches "^ *[0-9]+ +$pathToList(/day=[0-9]{2})?$" "$resultLine"
	done <<< "$result"

	# check I get only 29 lines
	nbLines=$(wc -l <<< "$result")
	assert_equals 29 "$nbLines"
	}


test_hdfs_dfs_du_datafiles() {
	# When the specified path contains dataFiles, 'hdfs dfs -du <path>' lists all dataFiles + their individual size
	# real behaviour:
	#	hdfs dfs -du /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02
	#		<size> /UAT/data_lake/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02/<dataFile>

	pathToList="$hdfsWorkDir/FEDERATED_INVENTORY/AGING/year=2024/month=02/day=02"
	result=$($HDFS dfs -du "$pathToList")
	while read size dataFile; do
#		echo "$size, $dataFile"
		# checking "$size" is a number
		assert '[ "$size" -ge 0 ]' "plop"

		# checking "$dataFile" is an actual file
		assert_status_code 0 '[ -f "$hdfsRoot$dataFile" ]'
	done <<< "$result"
	}


test_hadoopDistcp_combinationsOfOkKoSourcesDestinations() {
	while read hdfsDataToCopy s3Bucket expectedStatusCode; do
		longHadoopCommand="$HADOOP distcp \
			-Dfs.s3a.endpoint=S3_ENDPOINT \
			-Dfs.s3a.path.style.access=true \
			-Dhadoop.security.credential.provider.path=jceks://PATH/TO/KEY.jceks \
			-Dmapred.job.queue.name=QUEUE_NAME \
			-Dmapred.job.name='HDFS_to_S3_COPY' \
			'$hdfsDataToCopy' \
			's3a://$s3Bucket/'"
		assert_status_code $expectedStatusCode "$longHadoopCommand"
	done < <(cat <<-EOF
		/a/dir/that/does/not/exist		$defaultDestinationS3Bucket		1
		/a/dir/that/does/not/exist		edh-dev-raw-data				1
		/a/dir/that/does/not/exist		nonExistentS3Bucket				1
		$hdfsWorkDir/ABACUS				$defaultDestinationS3Bucket		0
		$hdfsWorkDir/ABACUS				edh-dev-raw-data				0
		$hdfsWorkDir/ABACUS				nonExistentS3Bucket				1
		EOF
		)
	}


test_hadoopDistcp() {
	local hdfsDataToCopy="$hdfsWorkDir/ABACUS/DEALS_DELTA/year=2024/month=02/day=12"
	local destinationS3Bucket="$defaultDestinationS3Bucket"

	actualSourceDirectory="$hdfsRoot/$hdfsDataToCopy"
	actualDestinationDirectory="$s3Root/$destinationS3Bucket"

	# copy data
	"$HADOOP" distcp \
		-Dfs.s3a.endpoint=S3_ENDPOINT \
		-Dfs.s3a.path.style.access=true \
		-Dhadoop.security.credential.provider.path=jceks://PATH/TO/KEY.jceks \
		-Dmapred.job.queue.name=QUEUE_NAME \
		-Dmapred.job.name='HDFS_to_S3_COPY' \
		"$hdfsDataToCopy" \
		"s3a://$destinationS3Bucket/"

	# check every file was copied
	while read hdfsSourceFile; do
		assert_status_code 0 '[ -e "$actualDestinationDirectory/$hdfsSourceFile" ]'
	done < <(cd "$actualSourceDirectory/.."; find $(basename "$hdfsDataToCopy"))
	}


test_hdfs_dfs_mkdir_singleNonExistingDir() {
	newDirectory='/newDirectory'
	$HDFS dfs -mkdir "$newDirectory"
	assert_status_code 0 '[ -d "$hdfsRoot$newDirectory" ]'
	# deleting the newly created directory to allow re-running this test
	[ -d "$hdfsRoot$newDirectory" ] && rmdir "$hdfsRoot$newDirectory"
	}


test_hdfs_dfs_mkdir_severalNonExistingDirs() {
	newDirectoryBasename='/newDirectory'
	$HDFS dfs -mkdir "$newDirectoryBasename"_{1..3}
	for newDir in "$hdfsRoot$newDirectoryBasename"_{1..3}; do
		assert_status_code 0 '[ -d "$newDir" ]'
		# deleting the newly created directory to allow re-running this test
		[ -d "$newDir" ] && rmdir "$newDir"
	done
	}


test_hdfs_dfs_mkdir_singleNonExistingDir_withPath() {
	newDirectory="$hdfsWorkDir/newDataSource/newDataSet/"
	$HDFS dfs -mkdir -p "$newDirectory"
	assert_status_code 0 '[ -d "$hdfsRoot$newDirectory" ]'

	# removing extra directories of the path that were created thanks to '-p'
	dirToRemove="$newDirectory"
	until [ "$dirToRemove" == "$hdfsWorkDir" ] ; do
#		echo "dirToRemove: '$dirToRemove', rmdir $hdfsRoot$dirToRemove"
		[ -d "$hdfsRoot$dirToRemove" ] && rmdir "$hdfsRoot$dirToRemove"
		dirToRemove=$(dirname "$dirToRemove")
	done
	}


test_hdfs_dfs_mkdir_severalNonExistingDirs_withPath() {
	newDirectoryBasename="$hdfsWorkDir/newDataSource/newDataSet/subDir"
	$HDFS dfs -mkdir -p "$newDirectoryBasename"_{1..3}
	for newDir in "$hdfsRoot$newDirectoryBasename"_{1..3}; do
		assert_status_code 0 '[ -d "$newDir" ]'
		# deleting the newly created directory to allow re-running this test
		[ -d "$newDir" ] && rmdir "$newDir"
	done

	# removing extra directories of the path that were created thanks to '-p'
	dirToRemove=$(dirname "$newDirectoryBasename")
	until [ "$dirToRemove" == "$hdfsWorkDir" ] ; do
#		echo "dirToRemove: '$dirToRemove', rmdir $hdfsRoot$dirToRemove"
		[ -d "$hdfsRoot$dirToRemove" ] && rmdir "$hdfsRoot$dirToRemove"
		dirToRemove=$(dirname "$dirToRemove")
	done
	}


test_hdfs_dfs_copyFromLocal_singleFile() {
	fileToCopyToHdfs='./myFile'
	touch "$fileToCopyToHdfs"
	hdfsDestinationDir="$hdfsWorkDir/"
	absolutePathToTestFileOnLocalFs="$hdfsRoot/$hdfsDestinationDir/$fileToCopyToHdfs"

	# checking the file doesn't exist beforehand, otherwise we can't be sure we copied anything
	assert_status_code 1 '[ -f "$absolutePathToTestFileOnLocalFs" ]'

	# copy non-existing file, must succeed
	$HDFS dfs -copyFromLocal "$fileToCopyToHdfs" "$hdfsDestinationDir"
	assert_status_code 0 '[ -f "$absolutePathToTestFileOnLocalFs" ]'

	# clean before leaving
	for fileToDelete in "$fileToCopyToHdfs" "$absolutePathToTestFileOnLocalFs"; do
		[ -f  "$fileToDelete" ] && rm "$fileToDelete"
	done
	}


test_hdfs_dfs_copyFromLocal_severalFiles() {
	fileToCopyToHdfs='./myFile'
	hdfsDestinationDir="$hdfsWorkDir/"
	absolutePathToHdfsDestinationDirOnLocalFs="$hdfsRoot/$hdfsDestinationDir"

	touch "$fileToCopyToHdfs"{1..3}

	# checking the files don't exist beforehand, otherwise we can't be sure we copied anything
	for i in {1..3}; do
		assert_status_code 1 '[ -f "$absolutePathToHdfsDestinationDirOnLocalFs/$fileToCopyToHdfs$i" ]'
	done

	# copy non-existing files, must succeed
	$HDFS dfs -copyFromLocal "$fileToCopyToHdfs"{1..3} "$hdfsDestinationDir"
	for i in {1..3}; do
		copiedFile="$absolutePathToHdfsDestinationDirOnLocalFs/$fileToCopyToHdfs$i"
		assert_status_code 0 '[ -f "$copiedFile" ]'

		# we won't need the 'source' + 'copied' files anymore, and this saves an extra 'for {1..3}' loop ;-)
		for fileToDelete in "$fileToCopyToHdfs$i" "$copiedFile"; do
			[ -f  "$fileToDelete" ] && rm "$fileToDelete"
		done
	done
	}
