# Kafka Producer / Consumer App

This is a Kafka app that runs a producer that generates random data and publishes it to a topic
that is consumed by consumer and inserted into the PostgreSQL database.

## Pre-requisities

This app was built and tested using these versions, which may be incompatible
with other versions.
1. Python (v3.6+)
2. Kafka (v2.3)
3. PostgreSQL (v11.5)

4. Environment Variables
  * KAFKA_URL - host name and port in the following format -- hostname,port
  * KAFKA_CA - ca.pem
  * KAFKA_SERVICE_CERT - service.cert
  * KAFKA_SERVICE_KEY - service.key
  * POSTGRES_URI - URI for PostgreSQL database

## Installation

1. Run the code below first to get necessary libraries installed for Python
<pre><code>pip install --user --requirement requirements.txt</code></pre>

2. Create table schema in your PostgreSQL database

OPTIONAL (Sample data is supplied in accounts.json file)

3. You may generate new data to send to consumer app
<pre><code>python generate_data.py</code></pre>

## Running app

### Sample run

<pre><code>python producer.py -t times-topic
python consumer.py -t times-topic -g demo-group -c demo-client</code></pre>

1. Run producer app first.

REQUIRED:

- -t topic name : specify which topic to use

OPTIONAL:

- -l : log to file "consumer.log". Otherwise it will log to
console.

- -v enable verbose : by default only ERRORs will be shown

<pre><code>python producer.py [-l -v]</code></pre>

2. Run consumer app

REQUIRED:

- -t topic name : specify which topic to use

OPTIONAL:

- -l : log to file "consumer.log". Otherwise it will log to
console.
- -v enable verbose : by default only ERRORs will be shown

- -g group_id : by default no group id is used

- -c client_id : by default kafka-python client id is used

<pre><code>python consumer.py -t topic_name -g groupid -c clientid [-l -v]</code></pre>

## Todo

- Add additional testing for database side
