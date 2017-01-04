#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <errno.h>

#include <tbsys.h>
#include <gtest/gtest.h>
#include "define.h"
#include "tfs_client_api.h"

using namespace tfs::common;
using namespace tfs::client;
using namespace std;

#define DEBUG	1
#define NO_DEBUG	0

#define KB		(1024)
#define MB		(1024*1024)
#define GB		(1024*1024*1024)
#define DEFAULT_TOTAL_SIZE	(6*MB)
#define DEFAULT_FILE_SIZE	(64)
#define DEFAULT_NSIP	"192.168.1.11:10000"
#define DEFAULT_LOG	"error"

TfsClient* tfs_client_ = NULL;
int	op_type_ = 0;
int	total_size_ = DEFAULT_TOTAL_SIZE;
int	file_size_ = DEFAULT_FILE_SIZE;
FILE *	list_file_ = NULL;
FILE *	time_file_ = NULL;
char list_file_name_[128];
char time_file_name_[128];
std::vector<string> filenamelist;
int	gLogLevel = NO_DEBUG;
int	gLoopTimes = 0;

#define LIST_FILE_NAME	"listfile"
#define TIME_FILE_NAME	"timefile"

typedef unsigned long	TIME_T;

/* internal */
int generate_data(char* buffer, const int32_t length);
int write_buffer(const char* buffer, const int32_t length);
int write_new_file(const char* buffer, const int32_t length);
int read_exist_file(const std::string& tfs_file_name, char* buffer, int32_t& length);

int store_file_name_list();
int load_file_name_list();
int get_file_name(char * file_name, size_t n);

int store_time(char * info, TIME_T t);
TIME_T getTime();
int timeToStr(time_t *now, char * nowStr);

void usage(char* name);
int parse_args(int argc, char *argv[]);

/* extern */
int average_read_time();
int integrate_test();
int batch_write(const int32_t length);
int batch_write_2(const int32_t total_size, const int32_t length);
int write_one_file(const int32_t length);
int read_one_file(const int32_t length);

enum OP_TYPE {
	OP_TYPE_MIN = 1,
	OP_BATCH_WRITE,
	OP_WRITE_ONE_FILE,
	OP_READ_ONE_FILE,
	OP_INTEGRATE_TEST,
	OP_AVERAGE_READ,
	OP_TYPE_MAX
};

int average_read_time()
{
	int i = 0, rv = 0;
	TIME_T start_time;
	TIME_T end_time;
	char head[256];

	memset(head, 0, 256);
	snprintf(head, 256, "average read time test"
			"(gtimes=%d, totallen=%ld, blocksize=%d)",
			gLoopTimes, (long)total_size_*KB, file_size_*KB);
	store_time(head, 0);

	start_time = getTime();
	srand(time(NULL) + rand() + pthread_self());
	for(i = 0; i < gLoopTimes; i++)
	{
		rv = read_one_file(file_size_);
		if (rv != 0)
		{
			printf("error read one file loop:%d, rv:%d\n", i, rv);
			return 0;
		}
	}
	end_time = getTime();
	store_time("begin time:", start_time);
	store_time("end time:", end_time);
	store_time("average second:", (end_time - start_time)/gLoopTimes);

	return 0;
}

int integrate_test()
{
	int i = 0, rv = 0;
	TIME_T start_time;
	TIME_T end_time;
	char head[256];

	memset(head, 0, 256);
	snprintf(head, 256, "integrate test(totallen=%ld, blocksize=%d)",
			(long)total_size_*KB, file_size_*KB);
	store_time(head, 0);

	printf("stage 1 batch write 6G data...\n");
	start_time = getTime();
	rv = batch_write_2(total_size_, file_size_);
	end_time = getTime();
	store_time("batch write begin time:", start_time);
	store_time("end time:", end_time);
	store_time("total second:", end_time - start_time);
	if (rv != 0)
	{
		printf("error batch_write rv:%d\n", rv);
		return 0;
	}

	rv = store_file_name_list();
	if (rv != 0)
	{
		printf("error store file_name_list rv:%d\n", rv);
		return 0;
	}

	printf("stage 2 read 10000 files, then count average time...\n");
	start_time = getTime();
	srand(time(NULL) + rand() + pthread_self());
	for(i = 0; i < 10000; i++)
	{
		rv = read_one_file(file_size_);
		if (rv != 0)
		{
			printf("error read one file loop:%d, rv:%d\n", i, rv);
			return 0;
		}
	}
	end_time = getTime();
	store_time("read 10000 files begin time:", start_time);
	store_time("end time:", end_time);
	store_time("average second:", (end_time - start_time)/10000);

	return 0;

}

int batch_write(const int32_t length)
{
	return batch_write_2(total_size_, length);
}

int batch_write_2(const int32_t total_size, const int32_t length)
{
	char * buf = NULL;
	int times = total_size / length;
	int len = length * KB;
	int i = 0, rv = TFS_SUCCESS;
	std::string file;
	
	int pagesize = getpagesize();
	posix_memalign((void **)&buf, pagesize, len);

	generate_data(buf, len);

	printf("loop tims: %d\n", times);
	for (i = 1; i < times + 1; i++)
	{
		//generate_data(buf, len);
		*(int *)buf = i;
		rv = write_new_file(buf, len);
		if (rv != TFS_SUCCESS)
		{
			printf("loop %d write data(len=%d) fail.rv=%d\n",
					i, len, rv);
			break;
		}
		file = std::string(tfs_client_->get_file_name());
		if (gLogLevel)
			cout << file << endl;
		filenamelist.push_back(file);
	}

	free(buf);
	return rv;
}

int write_one_file(const int32_t length)
{
	return batch_write_2(length, length);
}

int read_one_file(const int32_t length)
{
	char * buf = NULL;
	int32_t len = length * KB;
	int i = 0, rv = TFS_SUCCESS;
	
	if (filenamelist.size() == 0)
	{
		printf("list file is null...\n");
		return 0;
	}

	int pagesize = getpagesize();
	posix_memalign((void **)&buf, pagesize, len);

	int bingo_pos = 0;
	
	if (filenamelist.size() > 1)
	{
		//srand(time(NULL) + rand() + pthread_self());
		bingo_pos = rand() % filenamelist.size();
	}

	rv = read_exist_file(filenamelist.at(bingo_pos).c_str(), buf, len);
	if (rv != TFS_SUCCESS)
	{
		printf("read file(name:%s,len=%d) fail.rv=%d\n",
				filenamelist.at(bingo_pos).c_str(), len, rv);
	}

	if (buf)
		free(buf);
	return 0;
}

int generate_data(char* buffer, const int32_t length)
{
  srand(time(NULL) + rand() + pthread_self());
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int write_buffer(const char* buffer, const int32_t length)
{
  int32_t num_wrote = 0;
  int32_t left = length - num_wrote;

  int ret = TFS_SUCCESS;

  while (left > 0)
  {
    ret = tfs_client_->tfs_write(const_cast<char*>(buffer) + num_wrote, left);
    if (ret < 0)
    {
	  printf("tfs_write error. rv=%d\n", ret);
      return ret;
    }
    if (ret >= left)
    {
      return left;
    }
    else
    {
      num_wrote += ret;
      left = length - num_wrote;
    }
  }

  return num_wrote;
}

int write_new_file(const char* buffer, const int32_t length)
{
  int32_t ret = TFS_SUCCESS;
  ret = tfs_client_->tfs_open(NULL, "", WRITE_MODE);
  if (ret != TFS_SUCCESS)
  {
	  printf("tfs_open error. rv=%d\n", ret);
    return ret;
  }

  ret = write_buffer(buffer, length);
  if (ret != length)
  {
    return ret;
  }
  //cout << "write data successful" << endl;

  ret = tfs_client_->tfs_close();
  if (ret != TFS_SUCCESS)
  {
	  printf("tfs_close error. rv=%d\n", ret);
    return ret;
  }

  return TFS_SUCCESS;
}

int read_exist_file(const std::string& tfs_file_name, char* buffer, int32_t& length)
{
  int ret = tfs_client_->tfs_open((char*) tfs_file_name.c_str(), "", READ_MODE);
  if (ret != TFS_SUCCESS)
  {
    printf("tfsOpen :%s failed\n", tfs_file_name.c_str());
    return ret;
  }

  FileInfo file_info;
  ret = tfs_client_->tfs_stat(&file_info);
  if (ret != TFS_SUCCESS)
  {
    tfs_client_->tfs_close();
    printf("tfsStat:%s failed\n", tfs_file_name.c_str());
    return ret;
  }

  length = file_info.size_;
  int32_t num_readed = 0;
  int32_t num_per_read = length; //min(length, 16384);
  uint32_t crc = 0;

  do
  {
    ret = tfs_client_->tfs_read(buffer + num_readed, num_per_read);
    if (ret < 0)
    {
      break;
    }
    crc = tfs_client_->crc(crc, buffer + num_readed, ret);
    num_readed += ret;
    if (num_readed >= length)
    {
      break;
    }
  }
  while (1);

  if ((ret < 0) || (num_readed < length))
  {
    printf("tfsread failed (%d), readed(%d)\n", ret, num_readed);
    ret = TFS_ERROR;
    goto error;
  }

  if (crc != file_info.crc_)
  {
    printf("crc check failed (%d), info.crc(%d)\n", crc, file_info.crc_);
    ret = TFS_ERROR;
    goto error;
  }
  else
  {
    ret = TFS_SUCCESS;
  }

error:
  tfs_client_->tfs_close();
  return ret;
}

int store_file_name_list()
{
	int i = 0;
	char buf[20];

	if (list_file_ == NULL)
		list_file_ = fopen(list_file_name_, "a+");
	if (list_file_ == NULL)
	{
		printf("open list file errno:%d\n", errno);
		return errno;
	}

	for(i = 0; i < filenamelist.size(); i++)
	{
		strncpy(buf, filenamelist.at(i).c_str(), 20);
		strcat(buf, "\n");

		if (gLogLevel)
			printf("%s", buf);
		if (fwrite(buf, strlen(buf), 1, list_file_) != 1)
		{
			printf("fwrite filename error:%s\n", buf);
			return 1;
		}
	}

	fclose(list_file_);
	list_file_ = NULL;
	return 0;
}

int load_file_name_list()
{
	char file_name[20];
  	size_t namelen = 20;
	std::string file;

	if (list_file_ == NULL)
		list_file_ = fopen(list_file_name_, "a+");
	if (list_file_ == NULL)
	{
		printf("open list file errno:%d\n", errno);
		return errno;
	}

  	while(get_file_name(file_name, namelen) == 0)
	{
		if (gLogLevel)
			printf("file_name:%s\n", file_name);
		file = std::string(file_name);
		filenamelist.push_back(file);
	}
	
	fclose(list_file_);
	list_file_ = NULL;
	return 0;
}


int get_file_name(char * file_name, size_t n)
{
	char * name = NULL;
	size_t len = 0;
	ssize_t sz = getline(&name, &len, list_file_);

	if (sz == -1)
	{
		if (gLogLevel)
			printf("arrive file tail\n");
		return 1;
	}

	strncpy(file_name, name, sz-1);
	file_name[sz] = 0;

	if (gLogLevel)
		printf("get line:%s, sz = %d\n", file_name, sz);

	return 0;
}

int store_time(char * info, TIME_T t)
{
	int len = 0;
	char * buf = NULL;

	if (info == NULL)
	{
		len = 32;
		buf = (char *)malloc(len);
		snprintf(buf, len, "%ld\n", t);
	}
	else if (t == 0)
	{
		len = strlen(info) + 2;
		buf = (char *)malloc(len);
		snprintf(buf, len, "%s\n", info);
	}
	else
	{
		len = strlen(info) + 32;
		buf = (char *)malloc(len);
		snprintf(buf, len, "%s: %ld\n", info, t);
	}
	fwrite(buf, strlen(buf), 1, time_file_);
	free(buf);

	return 0;
}


TIME_T getTime()
{
	struct timeval t;
	gettimeofday(&t, NULL);
	return (((TIME_T)t.tv_sec) * 1000000 + (TIME_T)t.tv_usec);
}

int timeToStr(time_t *now, char *nowStr)
{
	struct tm r;
	memset(&r, 0, sizeof(r));
	time(now);
	if(localtime_r((const time_t *)now, &r) == NULL)
	{
		printf("localtime_r error:%d\n", errno);
		return 1;
	}
	sprintf(nowStr,"%04d%02d%02d%02d%02d%02d",
			r.tm_year+1900, r.tm_mon, r.tm_mday,
			r.tm_hour, r.tm_min, r.tm_sec);
	return 0;
}

void usage(char* name)
{
  printf("Usage: %s -s nsip:port -v version -l log_level -t totalsize -b blocksize -o op_type -w loop_times -f list_file_name:\n", name);
  printf("\t-s nsip:port:\tname server ip:port\n");
  printf("\t-v version:\tfsl,tfs\n");
  printf("\t-l log_level:\terror,warn,info,debug\n");
  printf("\t-t totalsize:\tthe size of total block, unit is GB\n");
  printf("\t-b blocksize:\tthe size of each block, unit is KB\n");
  printf("\t-w loop times:\tunit is KB\n");
  printf("\t-o op_type:\n");
  printf("\t\tOP_BATCH_WRITE\t%d\n",OP_BATCH_WRITE);
  printf("\t\tOP_WRITE_ONE_FILE\t%d\n",OP_WRITE_ONE_FILE);
  printf("\t\tOP_READ_ONE_FILE\t%d\n",OP_READ_ONE_FILE);
  printf("\t\tOP_INTEGRATE_TEST\t%d\n",OP_INTEGRATE_TEST);
  printf("\t\tOP_AVERAGE_READ\t%d\n",OP_AVERAGE_READ);
  printf("\t-f list_file_name\tread or unlink must specify this\n");
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char nsip[256];
  char version[8];
  char filename[128];
  int i = 0;

  TBSYS_LOGGER.setLogLevel(DEFAULT_LOG);
  strncpy(nsip, DEFAULT_NSIP, 256);
  memset(version, 0, 8);
  memset(list_file_name_, 0, 256);
  memset(time_file_name_, 0, 256);
  memset(filename, 0, 128);
  while ((i = getopt(argc, argv, "w:s:v:l:t:b:o:f:i")) != EOF)
  {
    switch (i)
    {
      case 'w':
	gLoopTimes = atoi(optarg) * KB;
        break;
      case 's':
        strcpy(nsip, optarg);
        break;
      case 'v':
        strncpy(version, optarg, 7);
        break;
      case 'l':
  	TBSYS_LOGGER.setLogLevel(optarg);
	if (strcmp(optarg, "debug") == 0)
		gLogLevel = DEBUG;
        break;
      case 't':
	total_size_ = atoi(optarg) * MB;
        break;
      case 'b':
	file_size_ = atoi(optarg);
        break;
      case 'o':
	op_type_ = atoi(optarg);
	if (op_type_ < OP_TYPE_MIN || op_type_ > OP_TYPE_MAX)
	{
		usage(argv[0]);
		return TFS_ERROR;
	}
	break;
      case 'f':
  	strncpy(filename, optarg, 128);
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }

  if(op_type_ == OP_BATCH_WRITE ||
		  op_type_ == OP_WRITE_ONE_FILE ||
		  op_type_ == OP_INTEGRATE_TEST)
  	sprintf(list_file_name_, "%s-%s-%dK-%ld",
			LIST_FILE_NAME, version, file_size_, getTime());
  else if (filename[0] == 0)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  else
  {
  	sprintf(list_file_name_, "%s", filename);
	load_file_name_list();
  }
  sprintf(time_file_name_, "%s-%s-%dK", TIME_FILE_NAME, version, file_size_);

  time_file_ = fopen(time_file_name_, "a+");
  if (time_file_ == NULL)
  	return errno;

  if ('\0' == version[0])
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  tfs_client_ = new TfsClient();
  int iret = tfs_client_->initialize(nsip);
  if (iret != TFS_SUCCESS)
  {
     return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
	char * mem1= 0;
	char * mem2 = 0;
	char * mem3 = 0;
	char * mem4 = 0;
	char * mem5 = 0;
	char * mem6 = 0;

	/* for test environment */
	mem1 = (char *)malloc(GB/2);
	mem2 = (char *)malloc(GB/2);
	mem3 = (char *)malloc(GB/2);
	mem4 = (char *)malloc(GB/2);
	mem5 = (char *)malloc(GB/2);
	mem6 = (char *)malloc(GB/2);
	if (mem1 == NULL || mem2 == NULL || mem3 == NULL || mem4 == NULL ||
			mem5 == NULL || mem6 == NULL)
	{
		printf("allocate 2.5G memory error\n");
		return 0;
	}

  int ret = parse_args(argc, argv);
  if (ret == TFS_ERROR)
  {
    printf("input argument error...\n");
  }

  printf("test begin...\n");
//close(2);
  switch (op_type_)
  {
	  case OP_BATCH_WRITE:
  //store_file_name(nowStr);
	  	ret = batch_write(file_size_);
		break;
	  case OP_WRITE_ONE_FILE:
		ret = write_one_file(file_size_);
		break;
	  case OP_READ_ONE_FILE:
		srand(time(NULL) + rand() + pthread_self());
		ret = read_one_file(file_size_);
		break;
	  case OP_INTEGRATE_TEST:
		ret = integrate_test();
		break;
	  case OP_AVERAGE_READ:
		ret = average_read_time();
		break;
	  default:
        	usage(argv[0]);
        	return TFS_ERROR;
		break;
  }

  if (time_file_)
	  fclose(time_file_);
  printf("test end rv = %d\n", ret);
  free(mem1);
  free(mem2);
  free(mem3);
  free(mem4);
  free(mem5);
  free(mem6);
  return 0;
}
