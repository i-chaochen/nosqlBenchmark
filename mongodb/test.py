import sys
sys.path.append('//anaconda/lib/python2.7/site-packages/pymongo-2.7.1-py2.7-macosx-10.5-x86_64.egg')
import pymongo
import time
import random
import pprint


#########################################################
#
#	"1G" = 1073741824 bytes
#	1kb = 1024 			1048576
#	2kb = 2048			524288
#	4kb = 4096 bytes	262144 
#	8kb = 8192 bytes	(131072 loops)	<-- 1073741824/8192
#	16kb = 16384 bytes	(65536 loops)
#	32kb = 32768 bytes	(32768)
#	64kb = 65536 bytes	(16384)
#########################################################

#db = pymongo.MongoClient()
#test_collection = db.test_collection

py_connection = pymongo.Connection()
#db = py_connection.test_db
test_collection = py_connection.test_db['test_collection']

# [0]*n*63, n is the size of kb
one_part = [0]*1*63
N = 1048576

before_t = time.time()

for i in xrange(N):
	test_collection.insert( {"i":one_part} )


after_t = time.time()

collection_stats = py_connection.test_db.command("collstats", "test_collection")

cost_t = after_t - before_t

print cost_t
pprint.pprint(collection_stats)



