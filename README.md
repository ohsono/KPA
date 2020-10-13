# kpa
## python admin tool for kafka(MSK)

### requirements:

```
-[] python3
-[x] install requirements.txt
    -[] kafka-python
    -[] wheel
    -[] PyYaml
    -[] botocore
-[] bootstrap endpoint (broker)
-[] check if *ssl* require or not
```

##### install python3

```
$ sudo yum install -y python3
```

##### install pip & required packages

```
$ sudo python3 -m pip install --upgrade
$ sudo python3 -m pip install -r requirements.txt
```

# to get boostrap endpoint fom MSK

```
TBD
```

##### Help

```
$ python3 ./kpa.py -h
usage: kpa.py [-h] [-v] -b BOOTSTRAP [-lts] [-dts DESC_TOPICS] [-lcgs]
                      [-dcg DESC_CONSUMER_GROUP] [-ofcg OFFSET_CONSUMER_GROUP]
                      [-c] [-d] [-t TOPIC_NAMES] [-f FILE_NAME] [--ssl]
                      [--dry] [--debug]

Kafka CLi tools (requirement python 3.x)

optional arguments:
  -h, --help            show this help message and exit
  -v                    display app version
  -b BOOTSTRAP          set bootstrap broker host
  -lts, --list_topics   display kafka topics list
  -dts DESC_TOPICS, --desc_topics DESC_TOPICS
                        display topic config description (i.e. all or topic_1,topic2)
  -lcgs, --list_consumer_groups
                        display consumer groups list
  -dcg DESC_CONSUMER_GROUP, --desc_consumer_group DESC_CONSUMER_GROUP
                        describe consumer group
  -ofcg OFFSET_CONSUMER_GROUP, --offset_consumer_group OFFSET_CONSUMER_GROUP
                        consumer group offsets
  -c, --create          create topic (optional)
  -d, --delete          delete topic (optional)
  -t TOPIC_NAMES, --topics TOPIC_NAMES
                        topic name(list)
  -f FILE_NAME          load file (json)
  --ssl                 Enable SSL
  --dry                 dry run
  --debug               turn debug mode on (option)
```

##### basic connection to kafka (example)

```
-- no ssl requirements
$ python3 ./kpa.py -b "bootstrap_server_address:port"

-- with SSL requirements
$ python3 ./kpa.py -b "bootstrap_server_address:port" --ssl
```

##### To create topics

```
-- you may edit the sample-topics.json file before create topics
$ python3 ./kpa.py -b "bootstrap_server_address:port" -c -f topics/<topics>.json --ssl
```

##### To delete topics

```
-- topic name (list), comma delimited
$ python3 ./kpa.py -b "bootstrap_server_address:port" -d -t "topic1,topic2,......"
```

##### To list topics

```
-- list topics
$ python3 ./kpa.py -b "bootstrap_server_address:port" --list_topics
```

##### To describe topic

```
-- topic_name (list), comma delimited
$ python3 ./kpa.py -b "bootstrap_server_address:port" --desc_topic "topic1,topic2,...."
```

##### To list consumer groups

```
-- list conusumer groups
$ python3 ./kpa.py -b "bootstrap_server_address:port" --list_consumer_groups
```

##### To enable debug mode

```
-- to add --debug flag at the end
 $ python3 ./kpa.py -b "bootstrap_server_address:port" --debug
```
