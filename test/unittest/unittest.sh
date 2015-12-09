#!/bin/sh

if [ "$#" -lt "1" ]; then
	echo "error: not enough parameters."
	echo "usage: $0 [workspace_dir]"
	exit -1
fi

CWD="$1"

PROJ_ROOT_DIR="${CWD}"

BIN_DIR="${PROJ_ROOT_DIR}/bin"
LIB_DIR="${PROJ_ROOT_DIR}/../common_lib/bin"
TEST_DIR="${PROJ_ROOT_DIR}/test/unittest"
TEST_TMP_DIR="/tmp/new_domain_redis_agent_test"
TEST_REPORT_DIR="${TEST_TMP_DIR}/report"
TEST_REPORT_FILE_PATH="${TEST_REPORT_DIR}/redis_agent_unit_result.xml"

BIN_FILES="new_domain_redis_agent.py"
LIB_FILES="conf_util.py new_domain_service_common.py new_domain_service_util.py redis_util.py pattern_notifier.py mail_queue_util.py mail_util.py aws_s3_util.py"
TEST_SCRIPTS="unittest_new_domain_redis_agent.py"
TEST_FILES="subproc_fail.py subproc_basic.py subproc_sleep_basic.py"

TEST_PACKAGES="new_domain_redis_agent"

#===============================================================================
# run unittest
#===============================================================================

echo "begin unit test..."

# re-create test tmp folder
rm -rf ${TEST_TMP_DIR}
mkdir -p ${TEST_TMP_DIR}

# copy source to test tmp dir
cd ${BIN_DIR}
cp ${BIN_FILES} ${TEST_TMP_DIR}

# copy lib to test tmp dir
cd ${LIB_DIR}
cp ${LIB_FILES} ${TEST_TMP_DIR}

# copy test script to test tmp dir
cd ${TEST_DIR}
cp ${TEST_SCRIPTS} ${TEST_TMP_DIR}

# copy test files to test tmp dir
cp -rf ${TEST_DIR}/samples ${TEST_TMP_DIR}

# create test report dir
mkdir -p ${TEST_REPORT_DIR}

# run test with normal user
cd ${TEST_TMP_DIR}

# execute static code analysis
#/usr/bin/flake8 --ignore='E501,E2,E701,E721,E302,W806' ${BIN_FILES}
#if [ $? != 0 ]; then
#    echo static code analysis failed
#    exit -1;
#fi

# composite options
OPTIONS="-v -s -x"
#OPTIONS="${OPTIONS} --with-xunit --xunit-file=${TEST_REPORT_FILE_PATH}"
OPTIONS="${OPTIONS} --cover-erase --with-coverage --cover-package=${TEST_PACKAGES}"
OPTIONS="${OPTIONS} -w ${TEST_TMP_DIR} ${TEST_SCRIPTS}"
# call nosetests
nosetests ${OPTIONS}
test_result=$?
# convert xml report
coverage xml -o ${TEST_REPORT_DIR}/redis_agent_coverage.xml ${TEST_TMP_DIR}/new_domain_redis_agent.py

if [ "${test_result}" -ne "0" ]; then
	echo "error: unit test failed!"
	exit -1
else
	echo "unit test passed"
fi

exit 0
