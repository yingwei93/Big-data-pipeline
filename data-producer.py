#
# - connect to any kafka broker, which in need of kafka broker ip and port.
# - grab stock price
# - grab stock data once a second
# - change what stock to grab dynamically
#
import atexit # do sth before exit
import json
import time 
import logging

from apscheduler.schedulers.background import BackgroundScheduler #for catch data every second
from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import (
	KafkaError,
	KafkaTimeoutError
)

from flask import ( # for creating web api for different stocks
	Flask,
	request,
	jsonify
)

logging.basicConfig()
logger = logging.getLogger('data-producer')

# - TRACE, DEBUG, INFO, WARNING, ERROR. more and more serious
logger.setLevel(logging.DEBUG)

app = Flask(__name__) # web app which needs url to access
app.config.from_envvar('ENV_CONFIG_FILE') #environment variable
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT'] 
topic_name = app.config['CONFIG_KAFKA_TOPIC']

producer = KafkaProducer(bootstrap_servers = kafka_broker)

schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()


symbols = set() # prohibite user to readd stock

def shutdown_hook(): #kill the thread like producer before quit to save resources
	logger.debug('prepare to exist')
	producer.flush(10) #producer may still sending files, wait 10s for it to send them all
	producer.close()
	logger.debug('kafka producer closed')
	schedule.shutdown()
	logger.debug('scheduler shut down')

def fetch_price(symbol):
	logger.debug('starting to fetch stock price for %s' % symbol)
	price = json.dumps(getQuotes(symbol))
	logger.debug('retrived stock price %s' % price)
	producer.send(topic = topic_name, value = price, timestamp_ms = time.time())
	logger.debug('sent stock price for %s to kafka' % symbol)

@app.route('/<symbol>', methods = ['POST'])
def add_stock(symbol):
	#grab new stock
	if not symbol: # symbol format incorrect like null 
		return jsonify({ #return json, represent number is 400
				'error' : 'Stock symbol is required'
			}), 400
	if symbol in symbols: #if stock already added, do nothing
		pass
	else:
		symbol = symbol.encode('utf-8')
		symbols.add(symbol)
		logger.info('adding stock retrieve job for %s' % symbol)
		schedule.add_job(fetch_price, 'interval', [symbol], seconds = 1, id = symbol) # every second run fetch_price
	return jsonify(results = list(symbols)), 200

@app.route('/<symbol>', methods = ['DELETE'])
def  del_stock(symbol):
	#user want to delete stock
	if not symbol: # symbol format incorrect like null 
		return jsonify({ #return json, represent number is 400
				'error' : 'stock symbol is till required'
			}), 400
	if symbol not in symbols:
		pass
	else: 
		symbol = symbol.encode('utf-8')
		symbols.remove(symbol)
		schedule.remove_job(symbol)
	return jsonify(results = list(symbols)), 200


if __name__ == '__main__':	
	atexit.register(shutdown_hook)
	app.run(host = '0.0.0.0', port = app.config['CONFIG_APPLICATION_PORT'])