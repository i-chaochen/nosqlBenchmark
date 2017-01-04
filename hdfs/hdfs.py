

#hdfs_input_stream = 'x'*1024

total_num_f = 1

for i in xrange(total_num_f):

	hdfs_file = "hdfs{0}.txt".format(i)

	f = open(hdfs_file, 'w')
	f.write('x')

	f.close()

