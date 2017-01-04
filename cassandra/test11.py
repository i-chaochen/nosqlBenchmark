from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import sys
import time


	
'''
input to the function is max_data_size
value data_size increased from  1 kb to max_data_size
clear data base at end of each interation
'''
def performance_test(max_data_size):
	data_size = 1
	cluster = Cluster() # Cluster(['ip address'])
	session = cluster.connect()

	session.execute("""
		CREATE KEYSPACE testdb WITH replication =
		{'class':'SimpleStrategy', 'replication_factor':3};
		""")
	session.execute("""
		USE testdb;
		""")

	session.execute("""
		CREATE TABLE test1 (
			id int PRIMARY KEY,
			data text
			);
		""")

	while data_size<=max_data_size:
		data_str = 'x'*1024*data_size
		#print sys.getsizeof(data_str)
		print 'data size ', data_size, 'KB'
		print

		query = SimpleStatement(
		    	"INSERT INTO test1 (id, data) VALUES(%s,%s)",
		    	consistency_level = ConsistencyLevel.ONE)

		count = 1048576/data_size #1048576 = 1G, change the number if you want
		#count = 1048576
		start = time.time()

		for i in xrange(count):
			session.execute(
		    	#insert into test1(id, data) values(1,%s)
		    	query, (i, data_str)
			)
		end = time.time()

		print 'total numbers of write', count
		print 'elap time', end-start
		print 'elap time per write', (end-start)/count
		print 'elap time per wrtier in 1kb', ((end-start)/count)/data_size
		print
		print 'lets do read'
		print
		query = "SELECT * FROM test1 WHERE id= %s"

		start = time.time()
		for i in xrange(count):
			row= session.execute(query, [i])[0]
		end = time.time()
		
		print 'total numbers of read', count
		print 'elap time', end-start
		print 'elap time per read', (end-start)/count
		print 'elap time per read in 1kb', ((end-start)/count)/data_size
		print
		
		session.execute("TRUNCATE test1") #clear database
		data_size = data_size*2

performance_test(2)
