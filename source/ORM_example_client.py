from pymongo import MongoClient
import MySQLdb
import paho.mqtt.client as mqtt
import json

def on_connect(client, userdata, rc):
	print "Client connected with result {0}".format(rc)
	client.subscribe("mongodb/kliento/position")
	client.subscribe("mysql/kliento/position")
	client.subscribe("redis/kliento/position")
	client.subscribe("postgresql/kliento/position")

def on_subscribe(client, userdata, mid, granted_qos):
	print "Subscribed with Quality of Service {0}".format(granted_qos)

def on_message(client, userdata, msg):
	print "Message received"
	data = msg.topic.split("/")
	client.insert(data[0], data[1], data[2], msg.payload)

def insert(dbms, database, table, payload):
	print "Information will insert on database {0} in table {1} at {2}".format(database, table, dbms)
	data = json.JSONDecoder().decode(payload)

	if dbms == "mongodb":
		client.mongo(database, table, data)
	elif dbms == "mysql":
		client.mysql(database, table, data)
	elif dbms == "redis":
		print "Inserted on {0}".format(dbms)
	elif dbms == "postgresql":
		print "Inserted on {0}".format(dbms)
	else:
		print "Unsupported DBMS"

def mongo(database, table, data):
	# Getting a client
	mongo = MongoClient('mongodb://localhost:27017')
	# Getting a database
	db = mongo[database]
	# Inserting data on a collection
	item = db[table].insert_one(data)
	# Showing result
	print "Inserted item {0} on {1}".format(item.inserted_id, dbms)

def mysql(database, table, data):
	# Getting a client
	mysql = MySQLdb.connect(host = "localhost", user = "root", passwd = "root", db = database)
	# Creating a cursor
	cursor = mysql.cursor()
	# Insert data
	cursor.execute("INSERT INTO " + table + " (latitude, longitude) VALUES("+ str(data["latitude"]) + ","+ str(data["longitude"]) +")")
	# Confirm changes
	mysql.commit()
	# Closing connections
	cursor.close()
	mysql.close()
	# Showing result
	print "Inserted tuple {0} on {1}".format(cursor.lastrowid, dbms)

client = mqtt.Client()
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
client.insert = insert
client.mongo = mongo
client.mysql = mysql

client.connect("45.55.210.26", 1883, 60)
client.loop_forever()