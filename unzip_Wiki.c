// Description: This file works for downloading the data from wikimedia traffic data by using MPI
//mpicc -g -o getWiki_data getWiki_data.c
//mpiexec -f hosts -n 16 ./getWiki_data

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

    char nameOfGZ[26];
    char URL_forDay[82];
    char Download_gz_URL[100];

    char Unzip[200];
    char Delete_gz[200];

    char MakeDir[28];
    char wgetCommand[122];
    //char unZipCommand[114];
    //char RemoveCommand[114];

    int start = 1;
    int end = 245;

    //system("mkdir /tmp/WikiData");  //Create WikiData directory
    //584

      //year = 0 -> 1900
      //mon = 0 -> 1
      //day = 1 -> 1
      ptm->tm_year = 115; //2015
      ptm->tm_mon = 0;
      ptm->tm_mday = 1;
      mktime(ptm);
      strftime(date, 9, "%Y%m%d", ptm);
      memcpy(&year, &date[0], 4);
      year[4] = '\0';
      memcpy(&month, &date[4], 2);
      month[2] = '\0';

      for (int i = 1; i <= 365; i++)
      {
        for (int j = 0; j <= 23; j++)
        {
          if (j <= 9)
          {
            snprintf(Unzip, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz | hadoop fs -put - /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, date, j, year, month, date, j);
            snprintf(Delete_gz, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, date, j);
          }
          else
          {
            snprintf(Unzip, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz | hadoop fs -put - /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz", year, month, date, j, year, month, date, j);
            snprintf(Delete_gz, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, date, j);
          }
          printf("%s\n", Unzip);
          printf("%s\n", Delete_gz);
          printf("\n");
        }
        ptm->tm_mday += 1;
        mktime(ptm);
        strftime(date, 9, "%Y%m%d", ptm);
        memcpy(&year, &date[0], 4);
        year[4] = '\0';
        memcpy(&month, &date[4], 2);
        month[2] = '\0';
      }

      // snprintf(URL_forDay, 82, "https://dumps.wikimedia.org/other/pagecounts-raw/%s/%s-%s/pagecounts-%s", year, year, month, date);
      // snprintf(MakeDir, 28, "mkdir /tmp/WikiData/%s-%s", year, month);

      //system(MakeDir);


//       for (int j = 0; j < 24; j++) {
//         if (j < 10) {
//           snprintf(Download_gz_URL, 100, "%s-0%d0000.gz", URL_forDay, j);
//         }
//         else {
//           snprintf(Download_gz_URL, 100, "%s-%d0000.gz", URL_forDay, j);
//         }
//         printf("%s\n", Download_gz_URL);/*
//         memcpy(&nameOfGZ, &Download_gz_URL[62], 26);
//         nameOfGZ[26] = '\0';  //get name of the
//
//         snprintf(wgetCommand, 154, "wget %s -P /tmp/WikiData/%s-%s", Download_gz_URL, year, month);
//         system(wgetCommand);
// */
//       }

    return 0;
}
