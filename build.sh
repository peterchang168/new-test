#!/bin/sh

build_dir=$1

if [ $build_dir ]; then
	echo "build destionation =" $build_dir
else
	echo "No build_dir specified."
	echo
	echo "build.sh <BUILD_PATH>"
	exit -1
fi

cd $build_dir

# create pattern directory
if [ ! -d ptn ]; then
   mkdir ptn
fi
if [ "$?" -ne "0" ]; then
	echo "fail to create pattern drectory"
	exit -1
fi
# create raw download directory
if [ ! -d raw ]; then
    mkdir raw
fi
if [ "$?" -ne "0" ]; then
	echo "fail to create raw download drectory"
	exit -1
fi

#rpm -ivh rpm/boto-2.38.0-1.noarch.rpm

public_suffix_provider=https://publicsuffix.org/list/public_suffix_list.dat
export public_suffix_provider 
python bin/public_suffix_generator.py -c conf/public_suffix_generator.conf 
if [ "$?" -ne "0" ]; then
	echo "fail to generate public suffix pattern"
        #rpm -e boto-2.38.0-1.noarch 
	exit -1
fi

#rpm -e boto-2.38.0-1.noarch 
