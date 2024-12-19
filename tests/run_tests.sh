#!/usr/bin/env bash

BASH_UNIT="$HOME/Dev/GitHub/bash_unit/bash_unit"

while read testFile; do
	[[ "$testFile" =~ ^# ]] && continue		# comment test files in the list below to skip them
	"$BASH_UNIT" "$testFile"
done < <(cat <<-EOF
	test_hdfsTree.sh
	EOF
	)

# to run a single test :
# bash_unit -p <pattern> <testFile>
#	"$HOME/Dev/GitHub/bash_unit/bash_unit" -p missingSourceDestination test_hdfsTree.sh
#	"$HOME/Dev/GitHub/bash_unit/bash_unit" -p hadoopDistcp test_hdfsTree.sh
#	"$HOME/Dev/GitHub/bash_unit/bash_unit" -p mkdir test_hdfsTree.sh
#	"$HOME/Dev/GitHub/bash_unit/bash_unit" -p copyFromLocal test_hdfsTree.sh
