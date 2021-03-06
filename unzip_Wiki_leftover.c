// Description: This file works for downloading the data from wikimedia traffic data by using MPI
/*
gcc -o unzip_Wiki_leftover unzip_Wiki_leftover.c -std=c99
./unzip_Wiki_leftover
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
//#include <mpi.h>

int main() {
    //start at 0153.11.9  --> 2007/12/09
    //2015   --> 0163.0.1
    //end at 0153.11.3171   -->2016/08/05
    char date[9];
    time_t t = time(NULL);
    struct tm * ptm = gmtime(&t);

    char year[4];
    char month[2];

    // Used in this file
    char Unzip[200];
    char Delete_gz[200];
    char Test_exist[200];


    int cmd;

      //year = 0 -> 1900
      //mon = 0 -> 1
      //day = 1 -> 1
      //Set up the start date
      ptm->tm_year = 115; //2015
      ptm->tm_mon = 9; // 10
      ptm->tm_mday = 28; // 28
      mktime(ptm);
      strftime(date, 9, "%Y%m%d", ptm);
      printf("date: %s\n", date);
      memcpy(&year, &date[0], 4);
      year[4] = '\0';
      memcpy(&month, &date[4], 2);
      month[2] = '\0';

      for (int i = 1; i <= 4; i++)
      {
        for (int j = 0; j <= 23; j++)
        {
          if (j <= 9)
          {
            // Used for testing. Unzip from li_j8's dirctory to li_j8's dictory
            // snprintf(Unzip, 200 ,"hadoop fs -text ./Wiki/%s-%s/pagecounts-%s-0%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-0%d0000.txt", year, month, date, j, year, month, date, j);
            // snprintf(Delete_gz, 200 ,"hdfs dfs -rm ./Wiki/%s-%s/pagecounts-%s-0%d0000.gz", year, month, date, j);

            // Used for actually unzipping. Unzip from valdiv_n1's dirctory to li_j8's dictory
            snprintf(Unzip, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-0%d0000.txt", year, month, date, j, year, month, date, j);
            snprintf(Delete_gz, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, date, j);
          }
          else
          {
            // snprintf(Unzip, 200 ,"hadoop fs -text ./Wiki/%s-%s/pagecounts-%s-%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-%d0000.txt", year, month, date, j, year, month, date, j);
            // snprintf(Delete_gz, 200 ,"hdfs dfs -rm ./Wiki/%s-%s/pagecounts-%s-%d0000.gz", year, month, date, j);

            // Used for actually unzipping. Unzip from valdiv_n1's dirctory to li_j8's dictory
            snprintf(Unzip, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-%d0000.txt", year, month, date, j, year, month, date, j);
            snprintf(Delete_gz, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz", year, month, date, j);
          }

          cmd = system(Unzip);
          cmd = system(Delete_gz);
          printf("%s\n", Unzip);
          printf("%s\n", Delete_gz);
          printf("\n");

        }
        ptm->tm_mday += 1;
        mktime(ptm);
        strftime(date, 9, "%Y%m%d", ptm);
        // printf("date: %s\n", date);
        memcpy(&year, &date[0], 4);
        year[4] = '\0';
        memcpy(&month, &date[4], 2);
        month[2] = '\0';
      }

    return 0;
}
