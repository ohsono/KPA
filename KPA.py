#!/usr/bin/python3
import os
import sys
import json
import logging
# from confluent_kafka.admin import AdminClient, NewTopic
from KafkaCLI.client import KafkaAdminCli, Config, Error

FORMAT = '%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#out_hdlr = logging.StreamHandler(sys.stdout)
#logger.addHandler(out_hdlr)

__version__='1.0.0'
#default_repl_factor=3
#default_client_id='test'

def command_line():
    '''
    cli function
    '''
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter
                                        ,description='Kafka CLi tools (requirement python 3.x)')
    parser.add_argument('-v', action='version',version='%(aws-msk-cli)s ver {}'.format(__version__), 
            help='display app version')
    parser.add_argument('-b', dest='bootstrap', help='set bootstrap broker host', required=True)
    parser.add_argument('-lts', '--list_topics', action='store_true', dest='list_topics', 
            help='display kafka topics list', required=False)
    parser.add_argument('-dts', '--desc_topics', dest='desc_topics', 
            help='display topic config description (i.e. ''all'' or ''topic_1,topic2'')', required=False)
    parser.add_argument('-lcgs','--list_consumer_groups', action='store_true', dest='list_consumer_groups', 
            help='display consumer groups list', required=False)
    parser.add_argument('-dcg','--desc_consumer_group', dest='desc_consumer_group', 
            help='describe consumer group', required=False)
    parser.add_argument('-ofcg','--offset_consumer_group', action='store_true', dest='offset_consumer_group', 
            help='consumer group offsets', required=False)
    parser.add_argument('-g','--group', dest='consumer_group', help='consumer group name(list)', required=False),
    parser.add_argument('-pt','--partition', dest='partition_id', help='partition id', required=False),
#    parser.add_argument('--sintopic', dest='single_topic', help='select single topic name', required=False)
#    parser.add_argument('--test', action='store_true', dest='test_only', 
#            help='test for dry run (optional)', required=False)
    parser.add_argument('-c', '--create', dest='create_flag', action='store_true', 
            help='create topic (optional)', required=False)
    parser.add_argument('-d', '--delete', dest='delete_flag', action='store_true',
            help='delete topic (optional)', required=False)
    parser.add_argument('--all', dest='all_flag', action='store_true', help='delete all flag, must used with delete_flag')
    parser.add_argument('-t', '--topics', dest='topic_names', help='topic name(list)', required=False)
    parser.add_argument('-f', dest='file_name', help='load file (json)', required=False)
    parser.add_argument('--ssl', dest="enable_ssl", action="store_true", help='Enable SSL', required=False)
    parser.add_argument('--dry', dest="dry_flag", action="store_true", help="dry run", required=False)
    parser.add_argument('--debug', dest='debug_flag', action='store_true', help='turn debug mode on (option)', required=False)
#    parser.add_argument('-h', dest='help_flag', help ='print help page')
    #parser.print_help()
    args = parser.parse_args()
    return args

def main():

    default_client_id='aws-msk-cli_'+__version__
    new_topic_list=[]
    default_repl_factor=3
    count=0

    args = command_line()
#    print (args)
    logger.debug("parms {}".format(args))
#    if (args.help_flag):
#        args.print_help()

    params={
            'bootstrap': args.bootstrap,
            'clientId': default_client_id,
            'listTopics': args.list_topics,
            'descTopics': args.desc_topics,
            'listConsumerGroups': args.list_consumer_groups,
            'descConsumerGroup': args.desc_consumer_group,
            'offsetConsumerGroup': args.offset_consumer_group,
            'consumerGroup': args.consumer_group,
            'partitionId': args.partition_id,
            'topicNames': args.topic_names,
            'createFlag': args.create_flag,
            'deleteFlag': args.delete_flag,
            'allFlag': args.all_flag,
            'filename': args.file_name,
            'enableSSL': args.enable_ssl,
            'dryrun': args.dry_flag,
            'debugFlag': args.debug_flag
    }

    if(args.debug_flag):
        logger.setLevel(logging.DEBUG)
        logger.debug("====================starting debug mode ==================!")

        for key, value in params.items():
#            print ("key:{}, value:{}".format(key, value))
            logger.debug("key: %s, value:%s",key, value)
            
    K = KafkaAdminCli(**params)

    # create topic from json file
    if (args.create_flag and args.file_name):
        if ("json" in args.file_name):
            K._topic_create_from_json_file()
        if ("yaml" in args.file_name) or ("yml" in args.file_name):
            K._topic_create_from_yaml_file()

    if (args.delete_flag is True):
        if (args.topic_names is True):
            try:
                message = K.kafka_topic_delete()
            except Errors as e:
                print (e)
        elif (args.file_name is True):
            try:
                message = K.kafka_topic_delete_file()
            except Errors as e:
                print (e)
        elif (args.all_flag is True):
            try:
                message = K.kafka_topic_delete_all()
            except Errors as e:
                print (e)

        if message is not None:
            logger.info("topic_delete: {} completed with {}".format(args.topic_names,message))

    # list of consumer group from broker
    if (args.list_consumer_groups):
        lcgs=K.kafka_consumer_groups()
        lcgsc=0
        for cg in lcgs:
            lcgsc += 1
            print("{}:{}".format(lcgsc,cg[0]))

    if (args.desc_consumer_group):
        logger.debug("param: {}".format(args.desc_consumer_group))
        kdcg=K.kafka_describe_consumer_group()
        print ("describe_consumer: {}".format(kdcg))

    if (args.offset_consumer_group):
        logger.debug("param: {} {} {}".format(args.offset_consumer_group,
            args.consumer_group,
            args.partition_id))
        ocg=K.kafka_list_consumer_group_offsets()
        print ("offset: {}".format(ocg))

    # list of topics from broker
    if (args.list_topics):
       lt = K.kafka_list_topics()
       lt.sort()
       ltc=0
       logger.info("print type of List_topic: %s", type(lt))
       for tp in lt:
           ltc += 1
           logger.info('topics[{}]: {}'.format(ltc,tp))

    # descibe topics, output json foramt
    if (args.desc_topics == 'all') or (args.desc_topics is not None):
        dt = K.kafka_describe_topics()
        #dt.sort()
        logger.debug("type of desc topics: %s", type(dt))
        logger.debug("topics: {}".format(dt))
        dtc=0
        for tp in dt:
            #logger.info('detailed: {} \n'.format(tp))
            dtc += 1
            print ("describe-Topics[{}]: {} \n".format(dtc,json.dumps(tp, indent=4)))

if __name__ == "__main__":
    main()
