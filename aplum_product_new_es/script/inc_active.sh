#! /bin/bash

cd /home/aplum/inner/aplum_product_new_es
# check lock exist
lock_file=inc_active.lock
if [[ -f $lock_file ]]
then
    echo $lock_file is already exists
    exit 0
else
    touch $lock_file
fi

source /home/aplum/.bashrc
source activate python36

python app.py activityproductincrease >> /data/aplum/logs/eslog/incrementactivityproduct.log 2>&1
python app.py activityincrease >> /data/aplum/logs/eslog/incrementactivity.log 2>&1

# remove the lock
rm -f $lock_file
