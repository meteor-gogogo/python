#!/bin/bash

log_file="/data/aplum/logs/restart_consumer.log"

function start() {
    echo "start"
    echo "----------------"
    source /home/aplum/.bashrc
    source activate python36
    nohup python -u /home/aplum/inner/aplum_product_new_es/consumer/newproductconsumer.py > /data/aplum/logs/newproductconsumer.log  2>&1 &
}

TIMESTAMP(){
    echo $(date "+%Y-%m-%d %H:%M:%S")
}

restart_process_if_die(){
    # $1->process_name by grep, $2->python directory
    # $3->process python file name
    be_running=$(ps -ef | grep $1 | wc -l)
    if [ $be_running -eq 0 ];
    then
        echo "$(TIMESTAMP) $2 got down, now I will restart it" | tee -a $log_file
        echo "Now I am in $PWD"
        start
        if [ $? -eq 0 ];
        then
            echo "$(TIMESTAMP) $2 restart successfully" | tee -a $log_file
        fi
    else
        echo "$(TIMESTAMP) $2 is running, no need to restart"
    fi
}
monitor_process="[p]ython.*newproductconsumer.py"
py_file="newproductconsumer.py"
#while :
#do
restart_process_if_die $monitor_process $py_file
#done
