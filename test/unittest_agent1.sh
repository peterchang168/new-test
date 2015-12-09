#!/bin/sh

mkdir -p /tmp/agent1_test
cp ${PWD}/bin/agent1.py /tmp/agent1_test
cp ${PWD}/test/unittest/unittest_agent1.py /tmp/agent1_test
mkdir -p /tmp/agent1_test/report
nosetests -v -s -x --with-xunit --xunit-file=$CIRCLE_TEST_REPORTS/agent1_unit_result.xml --cover-erase --with-coverage --cover-package=agent1 -w /tmp/agent1_test/ unittest_agent1.py
coverage xml -o $CIRCLE_TEST_REPORTS/agent1_coverage.xml /tmp/agent1_test/agent1.py

