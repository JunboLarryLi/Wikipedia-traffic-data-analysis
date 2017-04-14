// Description: This file works for downloading the data from wikimedia traffic data by using MPI
//mpicc -g -o getWiki_data getWiki_data.c
//mpiexec -f hosts -n 17 ./getWiki_data

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <mpi.h>

#define DATA_MSG 0
#define NEWDATA_MSG 1

int nnodes;
int me; // node's rank
int recv_buff[3];
int cmd;
void init(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nnodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
}

void managernode_DistWork() {
  MPI_Status status;
  int i, days;
  days = 245;
  //dividing data among all the nodes except managernode
  //sub_size[0] = size
  //sub_size[1] = start number
  //sub_size[1] = end number
  int sub_size[3];
  sub_size[0] = days/(nnodes-1);
  sub_size[1] = 0;
  sub_size[2] = 0;

  int last_sub_size[3];
  last_sub_size[0] = sub_size[0] + (days - sub_size[0]*(nnodes-1));
  last_sub_size[1] = 0;
  last_sub_size[2] = 0;
  // send share of the downloading dates to worker nodes
  for (i = 1; i < nnodes-1; i++) {
    sub_size[1] = (i-1)*sub_size[0]+1;
    sub_size[2] = i*sub_size[0];
    MPI_Send (&sub_size, 3, MPI_INT, i, DATA_MSG, MPI_COMM_WORLD);
  }

  last_sub_size[1] = (i-1)*sub_size[0]+1;
  last_sub_size[2] = last_sub_size[1] + last_sub_size[0]-1;
  MPI_Send (&last_sub_size, 3, MPI_INT, i, DATA_MSG, MPI_COMM_WORLD);

}

void MasterPutHDFS() {
  MPI_Status status;
  char slaveToMaster[74];
  char rmSlaveTmpData[35];

  const char *a[17];
  a[0]="219a";
  a[1]="219b";
  a[2]="219c";
  a[3]="219d";
  a[4]="219e";
  a[5]="219f";
  a[6]="219g";
  a[7]="219h";
  a[8]="219i";
  a[9]="219j";
  a[10]="219k";
  a[11]="219l";
  a[12]="219n";
  a[13]="219o";
  a[14]="219p";
  a[15]="216g";
  a[16]="216f";

  int i;
  cmd = system("mkdir /tmp/Wiki_Data");
  //forloop from 1-16 missing 17
  for (i = 0; i < nnodes; i++) {
    snprintf(slaveToMaster, 73, "scp -r %s:/tmp/WikiData/ /tmp/Wiki_Data/", a[i]);
    snprintf(rmSlaveTmpData, 35, "ssh %s 'rm -r /tmp/WikiData/'", a[i]);

    printf("HELLOO this is %s.... Moving data to Hadoop2\n", a[i]);
    cmd = system(slaveToMaster);    //copy data to Hadoop2

    printf("Removing from Slave......\n");
    cmd = system(rmSlaveTmpData);   //erase data from slave

    printf("Moving from Hadoop to HDFS......\n");
    cmd = system("hdfs dfs -put /tmp/Wiki_Data/WikiData ./Wiki_Data/");  //move data to HDFS

    printf("Removing Data from Hadoop......\n");
    cmd = system("rm -r /tmp/Wiki_Data/WikiData");

  }
  cmd = system("rm -r /tmp/Wiki_Data");
}

void workernode()
{
  // int* sub_size; // a buffer that receives the data
  // sub_size = malloc(3*sizeof(int));
  MPI_Status status;

  int cmd, start, end, i, j;
  char date[9];
  char year[4];
  char month[2];
  //start at 0153.11.9  --> 2007/12/09
  //2015   --> 0163.0.1
  //end at 0153.11.3171   -->2016/08/05
  time_t t = time(NULL);
  struct tm * ptm = gmtime(&t);

  //char nameOfGZ[26];
  char URL_forDay[82];
  char Download_gz_URL[100];
  char MakeDir[28];
  char wgetCommand[122];

  //reeive message from manager node
  MPI_Recv(&recv_buff, 3, MPI_INT, 0, DATA_MSG, MPI_COMM_WORLD, &status);

  start = recv_buff[1];
  end = recv_buff[2];

//  printf("Rank: %d: list looks like: %d, %d, %d\n ", me, recv_buff[0], start, end);
  cmd = system("mkdir /tmp/WikiData");  //Create WikiData directory
  //584
  //first run 17, second run 104 + 17
  for (i = start; i <= end; i++)
  {
    ptm->tm_year = 0163;
    ptm->tm_mon = 0;
    ptm->tm_mday = 120 + i;
    mktime(ptm);
    strftime(date, 9, "%Y%m%d", ptm);
    memcpy(&year, &date[0], 4);
    year[4] = '\0';
    memcpy(&month, &date[4], 2);
    month[2] = '\0';
    snprintf(URL_forDay, 82, "https://dumps.wikimedia.org/other/pagecounts-raw/%s/%s-%s/pagecounts-%s", year, year, month, date);
    snprintf(MakeDir, 28, "mkdir /tmp/WikiData/%s-%s", year, month);

    cmd = system(MakeDir);
    for (j = 0; j < 24; j++) {
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
}

int main(int argc, char** argv)
{
  int i;
  init(argc, argv);
  /*  master: Distribute file range.
      slaves: Get files */
  if (me == 0) {
    printf("I am the Master of rank: %d\n", me);
    managernode_DistWork();
  } else {
    workernode();
  }
  /*BARRIER ==> all documents have been downloaded*/
  MPI_Barrier(MPI_COMM_WORLD);

  /* Get files into Hadoop and put them into HDFS*/
  if (me == 0) {
    MasterPutHDFS();
  }

  MPI_Finalize();
}
