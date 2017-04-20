// Description: This file works for downloading the data from wikimedia traffic data by using MPI
//gcc CURATE_DAY.c -o CURATE_DAY -std=c99
//./CURATE_DAY m d hr

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int cmd;

void removeData(int month)
{
  char removeCMD[40];
  printf("removing data from HDFS /user/li_j8/Wiki......\n");
  if (month < 10) {
    snprintf(removeCMD, 40, "hdfs dfs -rm -r /user/li_j8/Wiki/2015-0%d", month);
  } else {
    snprintf(removeCMD, 40, "hdfs dfs -rm -r /user/li_j8/Wiki/2015-%d", month);
  }
  cmd = system(removeCMD);  //move data to HDFS
}

void cleanDataMonth (int month) {
  char hadoopCMD[100];
  if (month < 10) {
    snprintf(hadoopCMD, 100, "hadoop jar target/cleandata-1.0-SNAPSHOT.jar ./Wiki/2015-0%d/* /user/valdiv_n1/cleanWiki/2015-0%d", month, month);
  } else {
    snprintf(hadoopCMD, 100, "hadoop jar target/cleandata-1.0-SNAPSHOT.jar ./Wiki/2015-%d/* /user/valdiv_n1/cleanWiki/2015-%d", month, month);
  }
  cmd = system(hadoopCMD);  //move data to HDFS

  removeData(month);
}

void workernode(int init_mon, int end_mon)
{
  for (int i = init_mon; i <= end_mon; i++) {
    cleanDataMonth(i);
  }
}

int main(int argc, char** argv)
{
  int init, end;
  if (argc == 3) {
    init = atoi(argv[1]);
    end = atoi(argv[2]);
  }
  else {
    printf("Insufficient arguments:\n Correct unsage: ./CURATE_DAY <month> <day> <starting hour>");
    exit(-1);
  }
  
  workernode(init, end);
}
