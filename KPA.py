#!/usr/bin/python3

import os
import sys
import json
import logging
from KafkaCLI.client import _Admin, Config

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
out_hdlr = logging.StreamHandler(sys.stdout)
logger.addHandler(out_hdlr)

__version__='1.0.0'

def command_line():
    '''
    cli function
    '''
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter
                                        ,description='Kafka CLi tools (requirement python 3.x)')
    parser.add_argument('-v', action='version',version='%(KPA) ver {}'.format(__version__), 
            help='display app version')
    parser.add_argument('-b', dest='bootstrap', help='set bootstrap broker host', required=True)
    parser.add_argument('--lsts', action='store_true', dest='list_topics', help='display kafka topics list',
            required=False)
    parser.add_argument('--lscg', action='store_true', dest='list_consumer_groups', 
            help='display consumer groups list', required=False)
#    parser.add_argument('--test', action='store_true', dest='test_only', 
#            help='test for dry run (optional)', required=False)
    parser.add_argument('-c', '--create', dest='create_flag', action='store_true', 
            help='create topic (optional)', required=False)
    parser.add_argument('-d', '--delete', dest='delete_flag', action='store_true', 
            help='delete topic (optional)', required=False)
    parser.add_argument('-f', dest='file_name', help='load file (json)', required=False)
    parser.add_argument('--ssl', dest="enable_ssl", action="store_true", help='Enable SSL', required=False)
    parser.add_argument('--dry', dest="dry_flag", action="store_true", help="dry run", required=False)
    parser.add_argument('--debug', dest='debug_flag', action='store_true', help='turn debug mode on (option)', required=False)
#    parser.add_argument('-h', dest='help_flag', help ='print help page')
    #parser.print_help()
    args = parser.parse_args()
    return args

def main():

    default_client_id='KPA_'+__version__
    new_topic_list=[]
    default_repl_factor=3

    args = command_line()
    print (args)

    params={
            'bootstrap': args.bootstrap,
            'clientId': default_client_id,
            'listTopics': args.list_topics,
            'listConsumerGroups': args.list_consumer_groups,
            'createFlag': args.create_flag,
            'deleteFlag': args.delete_flag,
            'filename': args.file_name,
            'enableSSL': args.enable_ssl,
            'dryrun': args.dry_flag,
            'debugFlag': args.debug_flag
    }

    if(args.debug_flag):
        logger.setLevel(logging.DEBUG)
        logger.debug("starting debug mode ==================!")

        logger.debug("key: {} value:{}".format(key, value))
            
    K = _Admin(**params)

    if (args.create_flag and args.file_name):
        K._topic_create_from_json_file()
    if (args.list_topics):
        K.kafka_consumer_groups()

if __name__ == "__main__":
    main()
