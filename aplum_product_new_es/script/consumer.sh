#!/bin/bash
function killProcess() {
    NAME=$1
    echo $NAME
    #获取进程 PID
    PID=$(ps -ef | grep $NAME | grep -v grep| awk '{print $2}') 
    echo "PID: $PID"
    #杀死进程
    kill -9 $PID
}

function checkProcess() {
    NAME=$1
    be_running=$(ps -ef | grep $NAME| grep -v grep | wc -l)
    RETVAL=1
    if [ $be_running -ne 0 ];
    then
        RETVAL=0
    fi
}

function checkRunning() {
    checkProcess "newproductconsumer"
    if [ $RETVAL -eq 0 ]; then
        echo "newproductconsumer.py is running, we'll do nothing"
        exit
    fi
}

function status(){
    NAME="newproductconsumer"
    #be_running=$(ps -ef | grep $NAME| grep -v grep)
    #echo $be_running
    checkProcess $NAME
    if [ $RETVAL -eq 0 ]; then
        echo "newproductconsumer.py is running"
    else
        echo "newproductconsumer.py is down"
    fi
}

function start() {
    checkRunning
    echo "start"
    echo "----------------"
    source /home/aplum/.bashrc
    source activate python36
    nohup python -u /home/aplum/inner/aplum_product_new_es/consumer/newproductconsumer.py >> /data/aplum/logs/newproductconsumer.log  2>&1 &
}

function stop() {
    echo "stop"
    echo "----------------"
    killProcess "newproductconsumer"
}

function restart() {
    echo "restart"
    echo "----------------"
    stop
    start
}


case "$1" in
    start)
        echo "****************"
        start
        echo "****************"
        ;;  
    stop)
        echo "****************"
        stop
        echo "****************"
        ;;
    status)
        echo "****************"
        status
        echo "****************"
        ;;
    restart)
        echo "****************"
        restart
        echo "****************"
        ;;
        *)  
        echo "Usage: $0  {start|stop|restart|status}"
        ;;
esac


