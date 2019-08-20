#!/usr/bin/env bash

ELECTOR_WORKING_DIR=$(pwd)

ELECTOR_BIN_NAME='elector'

ELECTOR_PID_FILE_DIR='/run'
ELECTOR_PID_FILE_NAME='dms-elector.pid'
ELECTOR_PID_FILE=${ELECTOR_PID_FILE_DIR}/${ELECTOR_PID_FILE_NAME}

ELECTOR_EXE=${ELECTOR_WORKING_DIR}'/'${ELECTOR_BIN_NAME}

ELECTOR_CFG=$2


case $1 in
start)
    echo "starting elector..."
    echo "using configure file: ${ELECTOR_CFG}"

    if [ -f ${ELECTOR_PID_FILE} ]; then
        if kill -0 $(cat ${ELECTOR_PID_FILE}) > /dev/null 2>&1; then
            pid=$(cat ${ELECTOR_PID_FILE})
            echo "elector already running as process ${pid}"
            exit -1
        else
            echo "pid file exists but process does not exist, cleaning pid file"
            rm -f ${ELECTOR_PID_FILE}
        fi
    fi

    ${ELECTOR_EXE} ${ELECTOR_CFG} &

    if [ $? -eq 0 ]; then
        pid=$!
        if echo ${pid} > ${ELECTOR_PID_FILE}; then
            echo "elector started, as pid ${pid}"
            exit 0
        else
            echo "failed to write pid file"
            exit -1
        fi
    else
        echo "elector failed to start"
        exit -1
    fi
    ;;

stop)
    echo "stopping elector..."

    if [ ! -f ${ELECTOR_PID_FILE} ]; then
        echo "no elector to stop (could not find file ${ELECTOR_PID_FILE})"
        exit -1
    else
        kill -15 $(cat ${ELECTOR_PID_FILE})
        rm -f ${ELECTOR_PID_FILE}
        echo "elector stopped"
        exit 0
    fi
    ;;

status)
    if [ ! -f ${ELECTOR_PID_FILE} ]; then
        echo "no elector running"
        exit -1
    else
        if kill -0 $(cat ${ELECTOR_PID_FILE}) > /dev/null 2>&1; then
            pid=$(cat ${ELECTOR_PID_FILE})
            echo "elector running, as pid ${pid}"
            exit 0
        else
            echo "pid file exists but process does not exist, cleaning pid file"
            rm -f ${ELECTOR_PID_FILE}
            exit -1
        fi
    fi
    ;;

version)
    ${ELECTOR_EXE} --version
    ;;

*)
    echo "usage: $0 {start|stop|status|version}" >&2

esac