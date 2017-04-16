// Description: This file works for downloading the data from wikimedia traffic data by using MPI
//gcc CURATE_DAY.c -o CURATE_DAY -std=c99
//./CURATE_DAY m d hr

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int cmd;

void PutHDFS()
{
  printf("Moving from Hadoop to HDFS......\n");
  cmd = system("hdfs dfs -put /tmp/WikiData ./Wiki_Data/");  //move data to HDFS

  printf("Removing Data from Hadoop......\n");
  cmd = system("rm -r /tmp/WikiData");
}

void workernode(int mon, int day, int st_hr)
{
  int cmd, start, end, i, j;
  char date[9];
  char year[4];
  char month[2];
  //start at 0153.11.9  --> 2007/12/09
  //2015   --> 0163.0.1
  //end at 0153.11.3171   -->2016/08/05
  time_t t = time(NULL);
  struct tm * ptm = gmtime(&t);

  char URL_forDay[82];
  char Download_gz_URL[100];
  char MakeDir[28];
  char wgetCommand[122];

  cmd = system("mkdir /tmp/WikiData");  //Create WikiData directory

  ptm->tm_year = 0163;
  ptm->tm_mon = mon-1;
  ptm->tm_mday = day;
  mktime(ptm);
  strftime(date, 9, "%Y%m%d", ptm);
  memcpy(&year, &date[0], 4);
  year[4] = '\0';
  memcpy(&month, &date[4], 2);
  month[2] = '\0';

  snprintf(URL_forDay, 82, "https://dumps.wikimedia.org/other/pagecounts-raw/%s/%s-%s/pagecounts-%s", year, year, month, date);
  snprintf(MakeDir, 28, "mkdir /tmp/WikiData/%s-%s", year, month);

  cmd = system(MakeDir);

  for (j = st_hr; j < 24; j++) {
    if (j < 10) {
      snprintf(Download_gz_URL, 100, "%s-0%d0000.gz", URL_forDay, j);
    }
    else {
      snprintf(Download_gz_URL, 100, "%s-%d0000.gz", URL_forDay, j);
    }

    snprintf(wgetCommand, 122, "wget %s -P /tmp/WikiData/%s-%s", Download_gz_URL, year, month);

    cmd = system(wgetCommand);
  }
}

int main(int argc, char** argv)
{
  /*int month, day, start_hr, numOFDAYS;
  if (argc == 4) {
    month = atoi(argv[1]);
    day = atoi(argv[2]);
    start_hr = atoi(argv[3]);
  }
  else {
    printf("Insufficient arguments:\n Correct unsage: ./CURATE_DAY <month> <day> <starting hour>");
    exit(-1);
  }
  workernode(month, day, start_hr);*/
  workernode(8, 2, 5);
  PutHDFS();
  workernode(8, 18, 6);
  PutHDFS();
  workernode(9, 1, 9);
  PutHDFS();
  workernode(10,16, 5);
  PutHDFS();
  workernode(11, 15, 4);
  PutHDFS();
  workernode(11, 30, 9);
  PutHDFS();
  workernode(12, 15, 7);
  PutHDFS();
}
