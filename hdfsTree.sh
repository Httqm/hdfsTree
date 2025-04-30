#!/usr/bin/env bash

# TODO:
# - see examples from
#		https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html#dfs
#			https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html
#		https://data-flair.training/blogs/hadoop-hdfs-commands/

directoryOfThisScript="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. "$directoryOfThisScript/hdfsTree_config.sh"


usage() {
	scriptName=$(basename $(realpath $0))
	# I want to refer to the actual script, NOT the symlink, EVEN WHEN launching the script via its symlink

	cat <<-EOF
	This utility mocks
	- an HDFS storage
	- a S3 storage
	- some HDFS shell commands
	so that anyone can run 'hdfs dfs <command>' without Hadoop.

	To do so, 2 conditions :
	1. this script must be symlinked:
	     - 'hdfs   --> $scriptName'
	     - 'hadoop --> $scriptName'
	2. the path to the 'hdfs' + 'hadoop' symlinks must be appended to the \$PATH environment variable,
	    so that the 'hdfs' and 'hadoop' commands can be launched by external scripts.
	This will result in 'hdfs dfs <command>' and 'hadoop distcp' commands to be catched
	and interpreted by this script.

	Usage : <command> <option_1> [<option_2>]

	    command + option_1 + option_2 :

	    ./$scriptName
	        catch       show commands to catch 'hdfs dfs <command>'
	        create      create a file tree to mimic:
	            hdfs    an HDFS storage (see "\$hdfsTreeData" variable in 'hdfsTree_config.sh')
	            s3      a S3 bucket
	        delete      delete this file tree
	        h|help      show this [h]elp and exit

	    hdfs
	        dfs         used to differentiate 'hdfs shell' commands from other commands passed to this script
	            -count      mimic 'hdfs dfs -count ...'
	            -du         mimic 'hdfs dfs -du ...'
	            -ls         mimic 'hdfs dfs -ls ...'
	            -mkdir      mimic 'hdfs dfs -mkdir ...'
	            -rm         mimic 'hdfs dfs -rm ...'

	    hadoop
	        distcp


	Suggested usage :
	    1. create the simulated HDFS tree:                      ./$scriptName create hdfs
	    2. copy-paste commands to catch 'hdfs dfs <command>':   ./$scriptName catch
	    3. run and enjoy 'hdfs dfs <command>' commands
	    4. delete the simulated HDFS tree:                      ./$scriptName delete
	EOF
	}


showCatchCommands() {
	local symlinkName_hdfs='hdfs'
	local symlinkName_hadoop='hadoop'
	cat <<-EOF
	To create symlinks and setup \$PATH, paste and execute these commands:
	    ln -sf $(basename $0) $symlinkName_hdfs; ln -sf $(basename $0) $symlinkName_hadoop; PATH=\$PATH:"$directoryOfThisScript"

	Check:
	    $symlinkName_hdfs h	<== should display the script's help message
	EOF
	}


_createHdfsPathToFile() {
	local pathToCreate=$1	# 'year=yyyy/month=mm' or 'year=yyyy/month=mm/day=dd'
	local dataFile=$2		# day_dd
	local created=''
	[ ! -d "$pathToCreate" ]           && { mkdir -p "$pathToCreate"; created="$pathToCreate"; }
	[ ! -f "$pathToCreate/$dataFile" ] && { truncate -s "$datafileSize" "$pathToCreate/$dataFile"; created="$pathToCreate/$dataFile"; }
	echo "$created"
	}


createHdfsTree() {
	local nbObjects=0
	local startDay='2024-11-27'
	local endDay='2025-03-04'
	while read dataSource dataSet depth; do		# see 'hdfsTree_config.sh' for details about these parameters
		datasetDir="$hdfsRoot$hdfsWorkDir/$dataSource/$dataSet"
		theDay="$startDay"
		until [[ $theDay > "$endDay" ]]; do
		#	echo "$theDay"
			IFS='-' read -a myArray <<< "$theDay"

			case "$depth" in
				day)
					pathString="year=${myArray[0]}/month=${myArray[1]}/day=${myArray[2]}"
					pathToCreate="$datasetDir/$pathString"
					for datafileNumber in $(eval "echo {01..$nbDatafilePerDir}"); do
						result=$(_createHdfsPathToFile "$pathToCreate" "data_${myArray[0]}-${myArray[1]}-${myArray[2]}__$datafileNumber")
						[ -n "$result" ] && { echo "$result"; ((nbObjects++)); }
					done
					;;
				hour)
					for hour in {00..23}; do
						pathString="year=${myArray[0]}/month=${myArray[1]}/day=${myArray[2]}/hour=$hour"
						pathToCreate="$datasetDir/$pathString"
						for datafileNumber in $(eval "echo {01..$nbDatafilePerDir}"); do
							result=$(_createHdfsPathToFile "$pathToCreate" "data_${myArray[0]}-${myArray[1]}-${myArray[2]}_${hour}__$datafileNumber")
							[ -n "$result" ] && { echo "$result"; ((nbObjects++)); }
						done
					done
					;;
			esac
		    theDay=$(date -I -d "$theDay + 1 day")
		done
	done <<< "$hdfsTreeData"
	echo "$nbObjects HDFS directories/files created"
	}


createUserTrash() {
# hdfs dfs -ls /user/$USER/.Trash
#	Found 8 items
#	drwx------   - adedhdlkuat hdfs          0 2024-12-20 01:05 /user/adedhdlkuat/.Trash/241221010002
#	drwx------   - adedhdlkuat hdfs          0 2024-12-21 01:05 /user/adedhdlkuat/.Trash/241222010003
#	drwx------   - adedhdlkuat hdfs          0 2024-12-22 02:00 /user/adedhdlkuat/.Trash/241223010003
#	drwx------   - adedhdlkuat hdfs          0 2024-12-23 02:00 /user/adedhdlkuat/.Trash/241224010005
#	drwx------   - adedhdlkuat hdfs          0 2024-12-24 01:05 /user/adedhdlkuat/.Trash/241225010001
#	drwx------   - adedhdlkuat hdfs          0 2024-12-25 02:00 /user/adedhdlkuat/.Trash/241226010004
#	drwx------   - adedhdlkuat hdfs          0 2024-12-26 02:00 /user/adedhdlkuat/.Trash/241227010003
#	drwx------   - adedhdlkuat hdfs          0 2024-12-27 02:00 /user/adedhdlkuat/.Trash/Current
	true
	}


createS3Tree() {
	local nbS3DirectoriesCreated=0
	for s3Bucket in $s3Buckets; do
		directoryToCreate="$s3Root/$s3Bucket"
		[ ! -d "$directoryToCreate" ] && {
			mkdir -p "$directoryToCreate"
			((nbS3DirectoriesCreated++))
			}
	done
	echo "$nbS3DirectoriesCreated S3 directories created."
	}


deleteHdfsTree() {
	echo 'deleteHdfsTree'
	}


hdfsDfsCount() {
	# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#count
	local idOfLastArg=$#
	local pathToCount="$hdfsRoot${!idOfLastArg}"
	spaces='     '

	# checking whether we're asked to count a specific directory or 'all of them' with a command such as:
	#	hdfs dfs -count '/path/to/data/*'
	if [ "${pathToCount:(-1):1}" == '*' ] ; then

		pathToCount=${pathToCount%\*}
		for directoryToCount in "$pathToCount/"*; do
			nbDirectories=$(find "$directoryToCount" -type d | wc -l)
			nbFiles=$(find "$directoryToCount" -type f | wc -l)

			displayDirectory=${directoryToCount#$hdfsRoot}	# remove leading "$hdfsRoot" part
			displayDirectory=${displayDirectory//\/\//\/}	# remove double '/'
			echo "$spaces$nbDirectories$spaces$nbFiles${spaces}1234567$spaces$displayDirectory"
		done | column -s ' ' -t
	else
		nbDirectories=$(find "$pathToCount" -type d | wc -l)
		nbFiles=$(find "$pathToCount" -type f | wc -l)
		echo "$spaces$nbDirectories$spaces$nbFiles${spaces}1234567$spaces${!idOfLastArg}"
	fi
	}


hdfsDfsCopyFromLocal() {
	# /!\ known limitations :
	# 1. so far, this implementation of 'hdfs dfs -copyFromLocal' overwrites
	#	 existing destination files, unlike what the actual command does, i.e. my implementation behaves
	#	 as if 'hdfs dfs -copyFromLocal -f' was forced.
	#	 Fixing it would bring little to my current needs, which is why I left it as-is.
	# 2. not implemented: the case where the destination is 'destinationDir/filename'

	listOfFilesToCopy=''
	while [ "$#" -gt 1 ]; do
		listOfFilesToCopy="$listOfFilesToCopy $1"
		shift
	done
	local destination="$1"	# can be 'dir' (implemented) or 'dir/file' (NOT implemented!)
	local destinationOnLocalFs="$hdfsRoot/$destination"

	# $listOfFilesToCopy is a space-separated list of files (i.e. distinct arguments):
	#	./myFile1 ./myFile2 ./myFile3
	# Enclosing it within double quotes would change it into a single long string:
	#	"./myFile1 ./myFile2 ./myFile3"
	# There is no single file named './myFile1 ./myFile2 ./myFile3', hence no quotes.
	cp $listOfFilesToCopy "$destinationOnLocalFs"
	}


hdfsDfsDu() {
	# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#du
	idOfLastArg=$#
	pathToDu="$hdfsRoot${!idOfLastArg}"

	# when running 'hdfs dfs -du /path/to/directory', the line listing '/path/to/directory'
	# may/may not be visible in the output :
	#	- it's visible when '/path/to/directory' has dataFiles (i.e. '/path/to/directory/dataFile' exists)
	#	- hidden otherwise
	# which is why it's displayed / removed accordingly below
	nbDataFilesInSpecifiedDir=$(find "$pathToDu" -maxdepth 1 -type f | wc -l)
	if [ "$nbDataFilesInSpecifiedDir" -eq 0 ]; then
		# the specified dir has no dataFile
		escaped=$(sed 's|/|\\/|g' <<< "${!idOfLastArg}")
		result=$(du -d 1 --apparent-size "$pathToDu" | sed -r "/$escaped$/d")
	else
		# the specified dir has dataFile(s)
		result=$(du -d 1 --apparent-size "$pathToDu/"*)
	fi
	echo "$result" \
		| sed -r "s|$hdfsRoot||" \
		| tr '\t' ' ' \
		| sort -k 2 \
		| column -s ' ' -t
	}


hdfsDfsLs() {
	# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#ls
	# options to implement + test : -C -h -R -t -S -r

#	hdfs dfs -ls    /UAT/data_lake/shop/discs/year=2024/month=01
#	hdfs dfs -ls -R /UAT/data_lake/shop/discs/year=2024/month=01
#	hdfs dfs -ls -C /UAT/data_lake/shop/discs/year=2024/month=01

#	hdfs dfs -ls    /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=01
#	hdfs dfs -ls -R /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=01
#	hdfs dfs -ls -C /UAT/data_lake/ABACUS/IMPORTED_REQUESTS_DELTA/year=2024/month=01

	lsOptions='l'
	# the default option is 'l' because we're simulating 'hdfs dfs -ls' with 'ls'
	# and, by default, 'hdfs dfs -ls' outputs using a "long" format
	while getopts ":CR" opt; do
		case "$opt" in
			C)	# list names only (same as 'ls -1')
				lsOptions=$(sed 's/l/1/' <<< "$lsOptions")
				;;
			R)	# list recursively
				lsOptions+=R
				;;
			\?) error "[hdfsDfsLs] Invalid option: '-$OPTARG'"; exit 1	;;
		esac
	done

	idOfLastArg=$#
	pathInHdfs=${!idOfLastArg}	# the 'UAT/data_lake/<dataSource>/...' value passed to this function
	pathInHdfs=${pathInHdfs%/}	# removing trailing '/' in any

	pathToLs="$hdfsRoot$pathInHdfs"
	lsResult=$(ls --time-style=long-iso -"$lsOptions" "$pathToLs")

	case "$lsOptions" in
		l)	# default case, when 'hdfs dfs -ls /path/to/list' was invoked
			awk -v pathInHdfs=$pathInHdfs '
				/total/			{ next; }
				$0 ~ /^[-d]/	{$NF=pathInHdfs"/"$NF; print $0}' <<< "$lsResult" | column -s ' ' -t
			;;
		1)	# when 'hdfs dfs -ls -C /path/to/list' was invoked
			awk -v pathInHdfs=$pathInHdfs '
				/total/			{ next; }
				$0 ~ /^[-d]/	{$NF=pathInHdfs"/"$NF; print $0}' <<< "$lsResult" | column -s ' ' -t
			;;
		lR)
			# when 'hdfs dfs -ls -R /path/to/list' was invoked
			# NB: the output is not _exactly_ similar to the actual output but "does the job" so far
			#     see details in './tests/test_hdfsTree.sh/test_hdfs_dfs_ls_-R_depthHour()'
			awk -v pathInHdfs=$pathInHdfs \
				-v hdfsRoot=$(sed 's|/|\/|g' <<< "$hdfsRoot") '
				$0 ~ hdfsRoot	{ truc=gensub(hdfsRoot, "", 1, $0); truc=gensub(/:/, "", 1, truc); print truc; }
				$0 ~ /^-/		{ $NF=truc"/"$NF; print $0 }' <<< "$lsResult"
			;;
	esac
	}


hdfsDfsMkdir() {
	mkdirOptions=''
	while getopts ":p" opt; do
		case "$opt" in
			p)
				shift;
				mkdirOptions='-p'
				;;
			\?) error "[hdfsDfsMkdir] Invalid option: '-$OPTARG'"; exit 1	;;
		esac
	done
#	echo "remaining args: '$@'"
	for newDirectory in $@; do
#		echo "NEW DIR: '$newDirectory' ($hdfsRoot$newDirectory)"
		mkdir $mkdirOptions "$hdfsRoot$newDirectory"
		# no quotes around $mkdirOptions because when no options is passed, this gives :
		#	mkdir "" "</path/to/newDirectory>"
		# where "" is interpreted as the name of a directory to create and causes an error
	done
	}


hdfsDfsRm() {
	echo 'hdfs dfs -rm'
	}


hadoopDistcp() {
	# This function simulates commands such as :
	#		hadoop distcp \
	#			-Dfs.s3a.endpoint=S3_ENDPOINT \
	#			-Dfs.s3a.path.style.access=true \
	#			-Dhadoop.security.credential.provider.path=jceks://PATH/TO/KEY.jceks \
	#			-Dmapred.job.queue.name=QUEUE_NAME \
	#			-Dmapred.job.name='HDFS_to_S3_COPY' \
	#			'$hdfsDataToCopy' \						<== 'source'
	#			's3a://$s3Bucket/'"						<== 'destination'
	#
	# Retrieving the 'source' + 'destination' parameters from the 'hadoop distcp' command,
	# not interested in other parameters so far.
	nbArgs=$#
	argNb_hdfsDataToCopy=$((nbArgs-1))
	argNb_destinationBucket=$nbArgs
	local hdfsDataToCopy=${!argNb_hdfsDataToCopy}
	local destinationBucket=${!argNb_destinationBucket}
	destinationBucket=${destinationBucket#s3a://}
	destinationBucket=${destinationBucket%/}

	actualSourceDirectory="$hdfsRoot/$hdfsDataToCopy"
	actualDestinationDirectory="$s3Root/$destinationBucket"
	[ -d "$actualSourceDirectory" ] || {
		error "HDFS source directory '$hdfsDataToCopy' does not exist"
		exit 1
		}

	[ -d "$actualDestinationDirectory" ] || {
		error "S3 destination bucket '$s3Root/$destinationBucket' does not exist"
		exit 1
		}

	# copying '/path/to/HDFS/dataSource/dataSet/year=2024/month=02/day=12' gives, on the S3 bucket side :
	#	S3_Root/<bucketName>/day=12/<data>
	# which is not the cleanest it could be, but does the job so far.
	cp -r "$actualSourceDirectory" "$actualDestinationDirectory"
	}


validateCliCommandOption() {
	# This script can be called via several combinations of 'cliCommand' + 'cliOption', and
	# this function ensures the used combination of "$cliCommand $cliOption" is supported.
	local cliCommand=$(basename $1)
	local cliOption=$2
#	echo "$@, cliCommand: '$cliCommand', cliOption: '$cliOption'"

	commandIsSupported=0
	option1IsSupported=0
	combinationIsSupported=0
	while read supportedCommand supportedOption1; do
#		echo "$cliCommand ($supportedCommand), $cliOption ($supportedOption1)"
		[ "$cliCommand" == "$supportedCommand" ] && commandIsSupported=1
		[ "$cliOption" == "$supportedOption1" ] && option1IsSupported=1
		[ "$cliCommand" == "$supportedCommand" -a "$cliOption" == "$supportedOption1" ] && combinationIsSupported=1
	done < <(cat <<-EOF
		hadoop distcp
		hdfs dfs
		hdfs h
		hdfs help
		hdfsTree.sh catch
		hdfsTree.sh create
		hdfsTree.sh delete
		hdfsTree.sh h
		hdfsTree.sh help
		EOF
		)
	# TODO: check these in unit tests ?

	[ "$commandIsSupported" -eq 0 ] && {
		# This situation shouldn't occur because invoking this script with an unsupported
		# command name wouldn't even reach it. However, symlinking or or renaming this script
		# could trigger this test.
		error "This script shouldn't be invoked as '$cliCommand'."; usage; exit 1
		}

	[ "$option1IsSupported" -eq 0 ] && { error "unsupported option '$0 $cliOption'"; usage; exit 1; }

	[ "$combinationIsSupported" -eq 0 ] && {
		error "unsupported command combination '$cliCommand' + '$cliOption'"; usage; exit 1
		}
	}


error() {
	local errorMessage=$1
	echo -e "\e[1;31m$errorMessage\e[0m\n"
	}


main() {
	validateCliCommandOption "$(basename $0)" "$1"
	case "$1" in
# catch
# copyFromLocal
# create
# delete
# dfs
# distcp
# help
		catch)
			showCatchCommands ;;
		create)
			case "$2" in
				hdfs)	createHdfsTree; createUserTrash ;;
				s3)		createS3Tree ;;
				*)		error "Invalid or missing option: create '$2', did you mean :\n\"hdfs create hdfs\" ?\nor \"hdfs create s3\" ?"; usage; exit 1	;;
			esac
			;;
		delete)
			deleteHdfsTree ;;
		dfs)
			case "$2" in
				# commands are formatted like :
				#	hdfs dfs -<command> <options>
				#    $0   $1     $2      $3...$n
				# in blocks below, the double 'shift' is used to remove $1 ("dfs") and "$2" ("-<command>")
				# from the command line, so that "$@" only contains the "<options>" part
				-count)
					shift; shift
					hdfsDfsCount "$@"
					;;
				-copyFromLocal)
					shift; shift
					hdfsDfsCopyFromLocal "$@"
					;;
				-du)
					shift; shift
					hdfsDfsDu "$@"
					;;
				-ls)
					shift; shift
					hdfsDfsLs "$@"
					;;
				-mkdir)
					shift; shift
					hdfsDfsMkdir "$@"
					;;
				-rm)
					hdfsDfsRm	# TODO: to 'rm' we will actually 'mv' data in a .Trash dir
					;;
				*)	error "Invalid option: 'hdfs dfs $2'"; usage; exit 1	;;
			esac
			;;
		distcp)
			hadoopDistcp "$@"
			exit 0
			;;
		h|help)
			usage; exit 0	;;
		*)
			error "[main|*] Invalid option: '$1'"; usage; exit 1	;;
	esac
	}


main "$@"
