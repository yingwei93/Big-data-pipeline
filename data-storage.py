#
# - kafka broker address
# - kafka topic
# - cassandra broker location
# - cassandra keyspace/table
#

# - here we pass in ip and topic, simple; in industry, using naming service, to look up topic and keyspace

from cassandra.cluster import Cluster

from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse #for getting arguments from console
import logging
import json
import atexit

topic_name = None
kafka_broker = None
key_space = None
data_table = None
contact_points = None

logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def persist_data(stock_data, cassandra_session):
	"""
	[{
		"Index": "NASDAQ", 
		"LastTradeWithCurrency": "115.82", 
		"LastTradeDateTime": "2016-12-30T16:00:02Z", 
		"LastTradePrice": "115.82", 
		"LastTradeTime": "4:00PM EST", 
		"LastTradeDateTimeLong": "Dec 30, 4:00PM EST", 
		"StockSymbol": "AAPL", 
		"ID": "22144"
	}]
	"""
	try:
		logger.debug('start to save data %s', stock_data)
		parsed = json.loads(stock_data)[0] # it is an array
		symbol = parsed.get('StockSymbol')
		tradetime = parsed.get('LastTradeDateTime')
		price = float(parsed.get('LastTradePrice'))

		statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (data_table, symbol, tradetime, price)
		cassandra_session.execute(statement)
		logger.info('saved data to cassandra for %s, price %f, tradetime %s' % (symbol, price, tradetime))
	except Exception:
		logger.error('Failed to save data to cassandra %s', stock_data)

def shutdown_hook(consumer, session):
	try:
		consumer.close()
		session.shutdown()
		logger.info('kafka consumer and cassandra session shutdown')
	except Exception:
		logger.warn('failed to shutdown kafka consumer or cassandra session')

if __name__ == '__main__':
	# - setup commandline argument
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help = 'the kafka topic to subscribe from')
	parser.add_argument('kafka_broker', help = 'the location of the kafka broker')
	parser.add_argument('key_space', help = 'the keyspace for cassandra')
	parser.add_argument('data_table', help = 'the table to use')
	parser.add_argument('contact_points', help = 'the contact points for cassandra')

	# - parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	key_space = args.key_space
	data_table = args.data_table
	contact_points = args.contact_points

	# - create kafka consumer
	consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers=kafka_broker 
	)
		# bootstrap_servers: consumer contact with these servers at first, then broker will tell consumer the data on which server
		# in cassandra called contact points
		# in industry, don't contact with ip and port directly, using url like kafka.bootstrap.bittiger.com to contact
		# there are many servers inside this url, some down, can contact with others
	

	# - create a cassandra session
	cassandra_cluster = Cluster(
		contact_points=contact_points.split(',') # many servers, using ',' to split them
	)

	session = cassandra_cluster.connect()

	# - CQL
	# - %s : input keyspace name
	# - SimpleStrategy : once get first node, clockwise next two as replica
	# - durable_writes = 'true' : all write in replication done then return
	session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':'3'} AND durable_writes = 'true'" % key_space)
	# - text, timestamp, float are types in CQL
	session.set_keyspace(key_space)
	session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol, trade_time))" % data_table)

	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		persist_data(msg.value, session)






















