// Description: This file is used for downloading missing dates. (NO MPI INVOLVED HERE)
//gcc CURATE_DAY.c -o CURATE_DAY -std=c99
//./CURATE_DAY

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int cmd;

void PutHDFS()
{
  printf("Moving from Hadoop to HDFS......\n");
  cmd = system("hdfs dfs -put /tmp/WikiData /user/valdiv_n1/Wiki_Data/");  //move data to HDFS

  printf("Removing Data from Hadoop......\n");
  cmd = system("rm -r /tmp/WikiData");
}

void workernode(int mon, int day, int st_hr, int end_hr)
{
  int cmd, start, end, i, j;
  char date[9];
  char year[4];
  char month[2];
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

  for (j = st_hr; j <= end_hr; j++) {
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
  printf("Start!\n");
  workernode(8, 17, 6, 23);
  PutHDFS();
  workernode(8, 18, 0, 5);
  PutHDFS();
  workernode(10, 28, 0, 23);
  PutHDFS();
}
