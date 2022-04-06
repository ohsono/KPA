#!/usr/bin/python3.7

#import os
import sys
import json
import logging
#import yaml

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
