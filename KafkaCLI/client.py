#!/usr/bin/python3

#import os
import sys
import json
import logging
import yaml
# from confluent_kafka.admin import AdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, UnknownTopicOrPartitionError


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# out_hdlr = logging.StreamHandler(sys.stdout)
# logger.addHandler(out_hdlr)

class Error(Exception):
    pass

class TopicDeletionError(Error):
    pass

class Config(object):
    '''
        Config Class
        mapping external kwargs to internal properties
    '''
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)
    @property
    def _bootstrap(self):
        return getattr(self,"bootstrap")
    @property
    def _clientId(self):
        return getattr(self,"clientId")
    @property
    def _selectrTopic(self):
        return getattr(self,"selectTopic")
    @property
    def _listTopics(self):
        return getattr(self,"listTopics")
    @property
    def _ssl(self):
        return getattr(self,"enableSSL")
    @property
    def _file(self):
        return getattr(self,"filename")
    @property
    def _testOnly(self):
        return getattr(self,"dryrun")
    @property
    def _createTopics(self):
        return getattr(self,"createFlag")
    @property
    def _deleteTopics(self):
        return getattr(self,"deleteFlag")
    @property
    def _topicNames(self):
        return getattr(self,"topicNames")
    @property
    def _listConsumerGroups(self):
        return getattr(self,"listConsumerGroups")
    @property
    def _listTopics(self):
        return getattr(self,"listTopics")
    @property
    def _descTopics(self):
        return getattr(self,"descTopics")
    @property
    def _descConsumerGroup(self):
        return getattr(self,"descConsumerGroup")
    @property
    def _offsetConsumerGroup(self):
        return getattr(self,"offsetConsumerGroup")
    @property
    def _consumerGroup(self):
        return getattr(self,"consumerGroup")
    @property
    def _partitionId(self):
        return getattr(self,"partitionId")
#    def get_property(self, property_name):
#        if property_name not in self.kwargs.keys():
#            return None
#        return self.kwargs[property_name]

class KafkaAdminCli(Config):
    '''
        HMKL Kafkacli.
        create topic
        delete topic
        rebalance topics
    '''

    def __init__(self, **kwargs):
        super(KafkaAdminCli, self).__init__(**kwargs)
        self._topic_list=[]
        self._connection=None
        self._topic_name=None
        self._topic_partition=None
        self._topic_replica=None
        self._topic_assignments=None
        self._topic_configs=None

#        print(self.__dir__())

        if self._connection is None:
            if (self._ssl is None) or (self._ssl is False):
                self._connection=self.kafka_admin_connect(secure='PLAINTEXT')
            else:
                self._connection=self.kafka_admin_connect(secure='SSL')

#        print ("kwargs: {}\n, _testOnly: {} ".format(kwargs,self._testOnly.__getattribute__))
#        logger.debug( "dir: %s", kwargs.__dir__)
#        if self.dryrun is None:
#            self._testOnly = false
#        print("self._testOnly: {} ".format(self._testOnly))

    def kafka_admin_connect(self, secure=None):
        '''
        connect kafka
        '''
        logger.info("connecting via KafkaAdminClient")
        try:
            self._connection = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap, 
                    client_id=self.clientId,
                    security_protocol=secure)
        except NoBrokersAvailable as e:
            print ("kafka_admin_connect: {} NoBrokerAailable Error!".format(e))
        logger.debug("client connected: %s",self._connection)
        return self._connection

    def _topic_create_from_json_file(self):
        '''
        create topic from topic.json file
        return self.kafka_topic_create
        '''
        jsonData = self._open_file(self._file, "json")
#        self.dup_check(jsonData)
#        logging.debug('{}{}'.format(self._listTopics, self._file))

        list_topic= []
        for item in jsonData['topic']:
            print (item)
            ret = self._topic_formater_binder(item)
            list_topic.append(ret)
        print("list_topic: {}".format(list_topic))
        #size_of_dict = len(jsonData['topic'][0].keys())

        return self.kafka_topic_create(list_topic, self._testOnly)


    def _topic_create_from_yaml_file(self):
        '''
        create topic from topic.json file
        return self.kafka_topic_create
        '''
        jsonData = self._open_file(self._file, "yaml")
#        self.dup_check(jsonData)
#        logging.debug('{}{}'.format(self._listTopics, self._file))

        list_topic= []
        for item in jsonData['topic']:
            print (item)
            ret = self._topic_formater_binder(item)
            list_topic.append(ret)
        print("list_topic: {}".format(list_topic))
        #size_of_dict = len(jsonData['topic'][0].keys())

#        return self.kafka_topic_create(list_topic, self._testOnly)



    def _topic_formater_binder(self, Data):
        '''
        :toppic_formater_binder:
        topics with mixed formatted topic config to correct format
        return set_binder
        '''
        _topic_format = {
                "name": None,
                "partition": None,
                "replica": None,
                "assignments": None,
                "configs": None
                }

        for key, value in Data.items():
            #print ("K:{}, V:{}".format(key, value))
            if key == 'name':
                self._topic_name = value
            elif key == 'partition':
                self._topic_partition = value
            elif key == 'replica':
                self._topic_replica = value
            elif (key == 'assignments' and value != 'default'):
                self._topic_assignments = value[0]
            elif (key == 'configs'):
                self._topic_configs = value[0]

        _topic_format['name']=self._topic_name
        _topic_format['partition']=self._topic_partition
        _topic_format['replica']=self._topic_replica
        _topic_format['assignments']=self._topic_assignments
        _topic_format['configs']=self._topic_configs

        logger.debug ("topic_name: %s",self._topic_name)
        logger.debug ("topic_partition: %s",self._topic_partition)
        logger.debug ("topic_replica: %s",self._topic_replica)
        logger.debug ("topic_replica_assignment: [%s]",self._topic_assignments)
        logger.debug ("topic_configuration: [%s]",self._topic_configs)

        return self._newtopic_binding(
                _topic_format['name'],
                _topic_format['partition'],
                _topic_format['replica'],
                _topic_format['assignments'],
                _topic_format['configs'])

    def kafka_topic_create(self, topic_list, flag=None):
        '''
        create_topics
        '''
        if flag is not None:
            flag=self._testOnly
#        logger.debug("flag: %s, _testOnly: %s", flag.__self__, self._testOnly.__self__)
        try:
            self._connection.create_topics(new_topics=topic_list,
                                                    validate_only=flag)
        except TopicAlreadyExistsError as t:
            print("Type: ", type(t), "TopicAlreadyExistsError:: \n", t)
            pass

    def kafka_topic_delete(self):
        '''
        kafka_topic_delete
        '''
        logger.debug("_deleteTopic:{}, _testOnly:{}, _topicNames:{}".format(self._deleteTopics, self._testOnly, self._topicNames))
        if (self._deleteTopics):
            return self._topic_delete(topic_list=self._topicNames, flag=self._testOnly)
        else: 
            raise ("delete topic flag has not been triggerred")

    def _topic_delete(self, topic_list=None, flag=None):
        '''
        _topic_delete
        parms: topic_list (string)
        '''
        #topic_list=[]
        logger.debug("topic_list:{} is going to be deleted!".format(topic_list))
        if topic_list is None or len(topic_list)==0:
            raise TopicDeletionError("at least more than one topic need to be input")
        else:
            new_list=topic_list.split(",")
            logger.info("newlist: %s",new_list)

        if flag is not None:
            flag=self._testOnly
        try:
            self._connection.delete_topics(new_list)
        except UnknownTopicOrPartitionError as u:
            logger.debug("Error while deleteing topic: {}".format(type(u)))
            print("TypeL: ", type(u), "UnknownTopicOrPartitionError:: \n", u)

    def kafka_list_topics(self):
        logger.info("Listing Topics")
        return self._connection.list_topics()

    def kafka_describe_topics(self):

        _topic=[]
        
        if (self._descTopics == 'all') or len(self._descTopics) == 0:
            _topic=None
        else:
            logger.debug("type: {}, list: {}".format(type(self._descTopics), self._descTopics))
            _topic=self._descTopics.split(',')
        

        logger.info("Describe Topics")
        return self._connection.describe_topics(_topic)
          

    def kafka_consumer_groups(self, Broker_ids=None):
        #result2 = self._connection.list_consumer_groups(Broker_ids)
        logger.info("Listing Consumer Groups")
        return self._connection.list_consumer_groups(Broker_ids)

    def _load_Json(self, data):
        '''
        load json data to directionary
        param : json data
        return: json dictionary
        '''
        return json.load(data)

    def _open_file(self, filename, fileType):
        '''
        open file curor to read context
        return: file
        '''
        with open(filename, 'rt') as rtfile:
        #   print (type(file))
            if (fileType == "json"):
                outfile = self._load_Json(rtfile)
            elif (fileType == "yaml"):
                outyaml=yaml.safe_load(rtfile)
                logger.debug("yaml: %s",outyaml)
                # print (type(outyaml))
                outfile = outyaml
        logger.debug("output (dictionary): %s",outfile)
        return outfile

    def _newtopic_binding(self, topic_name, topic_parition, topic_replica, topic_replica_assign=None, topic_configs=None):
        '''
        binding newtopic with criteria of NewTopic class

        required: name(string), num_partitions(int), replication_factor(int)
        optional: replica_assignment(dict(int)), topic_configs(dict(str))

        return: instance of class
        '''
        NT = NewTopic(name=topic_name,
                      num_partitions=topic_parition,
                      replication_factor=topic_replica,
                      replica_assignments=topic_replica_assign,
                      topic_configs=topic_configs)

        return NT


    def kafka_api_version(self):
        '''
        get api version
        '''
        return self._connection.get_api_versions()


    def kafka_describe_consumer_group(self):
        '''
        describe consumer group
        :params: consumer_group_id (string)
        '''
        _consumer_group_id=[]

        if self._descConsumerGroup is None or len(self._descConsumerGroup) == 0:
            _consumer_group_id=None
        else:
            _consumer_group_id=self._descConsumerGroup.split(",")

        return self._connection.describe_consumer_groups(group_ids=_consumer_group_id)


    def kafka_list_consumer_group_offsets(self):
        '''
        list consumer group offsets
        '''
        if self._partitionId is None:
            partition_id=None
        else:
            partition_id=self._partitionId.split(",")
        
        if self._consumerGroup is None or self._consumerGroup == 'all' or self._consumerGroup == 'ALL':
            consumer_group_id=self.kafka_consumer_groups()
        else:
            consumer_group_id=self._consumerGroup.split(",")

        return self._connection.list_consumer_group_offsets(group_id=consumer_group_id, partitions=partition_id)


def main():
    topic_list = []

    file = "../topics/usps.events.v1.json"
    jsonData = open_file(file)
    for j in jsonData['topic']:
        tn = j['name']
        tp = j['partition']
        tr = j['replica']
        cf = j['configs'][0]
        bundle = newtopic_binding(tn, tp, tr, cf)
        topic_list.append(bundle)

    print ("{}".format(topic_list))

if __name__ == "__main__":
    main()
