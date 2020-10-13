#!/bin/bash
 
#########################################################
##
##  Check consumer group connection
##  author: Hochan Son
##  Date: 06/11/2020
##
##  Expected output is empty, stable.
##  (empty: empty  consumer group)
##  (stable: maybe one or more memeber in consumer groups)
##
#########################################################

broker_bootstrap=""
zookeeper_boostrap=""
 
echo $broker_bootstrap

if [ -z $broker_bootstrap ]; then
	read -p "please enter the broker bootstrap: " broker_bootstrap
fi

consumer_group_list=$(kafka-consumer-groups --bootstrap-server $broker_bootstrap --list)
 
echo $consumer_group_list
 

for cg in  $consumer_group_list
do  
    printf "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>checking on {$cg}<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
    kafka-consumer-groups --bootstrap-server $broker_bootstrap --group $cg --describe --state
    kafka-consumer-groups --bootstrap-server $broker_bootstrap --group $cg --describe --members
done

