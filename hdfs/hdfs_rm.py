import os


total_num = 1000

for i in xrange(total_num):

	hdfs_file = "hdfs16kb{0}.txt".format(i)

	os.remove('hdfs_file')