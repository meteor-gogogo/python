#!/bin/bash

CURRDIR=`pwd`/aplum_thrift
sed -i s/aplum_thrift\.qserver\.product_info\.ttypes\/library\.aplum_thrift\.qserver\.product_info\.ttypes\/g `grep 'aplum_thrift.qserver.product_info.ttypes' -rl $CURRDIR`


