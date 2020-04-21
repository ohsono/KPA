#!/usr/bin/python3

import os
import sys
import json
import logging
# from confluent_kafka.admin import AdminClient, NewTopic
from kafka.admin import NewTopic
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

#logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#out_hdlr = logging.StreamHandler(sys.stdout)
#logger.addHandler(out_hdlr)


class Config(object):
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
    def _topicList(self):
        return getattr(self,"listTopics")
    @property
    def _ssl(self):
        return getattr(self,"enableSSL")
    @property
    def _file(self):
        return getattr(self,"filename")
    def _testOnly(self):
        return getattr(self,"dryrun")

class _Admin(Config):
    '''
        create topic
        delete topic
        rebalance topics
    '''

#    def __init__(self):
#        self.connection_string = default_bootstrap
#        self.client_id = default_client_id
#        self.in_file = default_file


    def __init__(self, **kwargs):
        super(_Admin, self).__init__(**kwargs)
        self._topic_list=[]
        self._connection=None

        print(self.__dir__())

        if self._connection is None:
            if self._ssl is None:
                self._connection=self.kafka_admin_connect(secure='PLAINTEXT')
            else:
                self._connection=self.kafka_admin_connect(secure='SSL')

        if self.dryrun is None:
            self._testOnly = false

    def kafka_admin_connect(self, secure=None):
        '''
        connect kafka 
        '''
        self._connection = KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id=self.clientId, security_protocol=secure)
        return self._connection

    def _topic_create_from_json_file(self):
        '''
        create topic from topic.json file
        '''
        jsonData = self._open_file(self._file)

        for j in jsonData['topic']:
            tn = j['name']
            tp = j['partition']
            tr = j['replication']
            bundle = self.topic_binding(tn, tp, tr)
            logging.debug('{}{}{}'.format(tn, tp, tr))
            self._topic_list.append(bundle)
        return self.kafka_topic_create(self._topic_list, self._testOnly)


    def kafka_topic_create(self, topic_list, flag=None):
        '''
        create_topics
        '''
        try:
            self._connection.create_topics(new_topics=topic_list,
                                                    validate_only=flag)
        except TopicAlreadyExistsError as t:
            print("Type: ", type(t), "TopicAlreadyExistsError:: \n", t)
            pass

    def kafka_topic_delete(self, topic_list):
        '''
        delete_topics
        parms: topic_list (string)
        '''
        try:
            self._connection.delete_topics(topic_list)
        except UnknownTopicOrPartitionError as u:
            print("TypeL: ", type(u), "UnknownTopicOrPartitionError:: \n", u)
            pass

    def kafka_consumer_groups(self, Broker_ids=None):
            result2 = self._connection.list_consumer_groups(Broker_ids)
            print ("Listing Consumer Groups")
            print ("result: {}".format(result2))

    def dup_checker(self, JasonData):
        _duplist = []
        for k in JasonData["topic"].keys():
            if dict[k]==0:
                dict[k]=1
            elif dict[k]>0:
                dict[k]+=1
                _dup.append(k) 
        return _duplist

    def _load_Json(self, data):
        '''
        load json data to directionary
        param : json data
        return: json dictionary 
        '''
        return json.load(data)

    def _open_file(self, filename):
        '''
        open file curor to read context
        return: file
        '''
        with open(filename, 'rt') as rtfile:
            outfile = self._load_Json(rtfile)
        return outfile

    def topic_binding(self, topic_name, topic_parition, topic_replica):
        '''
        binding topic criteria of NewTopic class

        required: name(string), num_partitions(int), replication_factor(int)
        optional: replication_assignment(dict(int)), topic_configs(dict(str))

        return: instance of class
        '''
        NT = NewTopic(name=topic_name,
                      num_partitions=topic_parition,
                      replication_factor=topic_replica)
        return NT


'''
def main():
    topic_list = []

    file = "./kafka-topics.json"
    jsonData = open_file(file)
    for j in jsonData['topic']:
        tn = j['name']
        tp = j['partition']
        tr = 1
        bundle = topic_binding(tn, tp, tr)
        topic_list.append(bundle)


    print ("{}".format(topic_list))

if __name__ == "__main__":
    main()
'''
