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

void Manger_DistributeWork() {
  MPI_Status status;
  const char * a[nnodes-1];
  const char * dirs[nnodes-1];

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
  //a[11]="219l";

  dirs[0]="2015-01";
  dirs[1]="2015-02";
  dirs[2]="2015-03";
  dirs[3]="2015-04";
  dirs[4]="2015-06";
  dirs[5]="2015-07";
  dirs[6]="2015-08";
  dirs[7]="2015-09";
  dirs[8]="2015-10";
  dirs[9]="2015-11";
  dirs[10]="2015-12";
  //dirs[4]="2015-05";
  int i;
  char getThisMonth[62];
  char makeDir[30];
  char MasterToslaveTmp[48];
  char rmMasterTmpData[29];

  printf("Hello, I am hadoop2. I am making a directory in Tmp.....\n");
  cmd = system("mkdir /tmp/ThisMonth"); //make a directory in haoop2, where all operations happen here

  for (i = 0; i < nnodes-1; i++)
  {
    printf("Hadoop2: Getting %s from HDFS......\n", dirs[i]);
    snprintf(getThisMonth, 62, "hdfs dfs -get /user/valdiv_n1/cleanWiki/%s /tmp/ThisMonth", dirs[i]);
    cmd = system(getThisMonth);  //move data to hadoop2
    //**********************************
    printf("%s: Creating Dir in tmp......\n", a[i]);
    snprintf(makeDir, 30, "ssh %s 'mkdir /tmp/MyMonth'" , a[i]);
    cmd = system(makeDir);    //copy data to Hadoop2
    //**********************************
    printf("Hadoop2: Sending data to %s......\n", a[i]);
    snprintf(MasterToslaveTmp, 48, "scp -r /tmp/ThisMonth/%s %s:/tmp/MyMonth/",dirs[i], a[i]);
    cmd = system(MasterToslaveTmp);

    printf("Hadoop2: Copy finished! Now remove tmp data......\n");
    snprintf(rmSlaveTmpData, 29, "rm -r /tmp/ThisMonth/%s", dirs[i]);
    //get Month for slave i ito hadoop2's tmp
  }
  cmd = system("rm -r /tmp/ThisMonth"); //delete this project directory from hadoop2
}

void MasterPutHDFS() {
  MPI_Status status;
  char slaveToMaster[42];
  char rmSlaveTmpData[35];

  const char *a[nnodes-1];
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
  // a[11]="219l";
  // a[12]="219n";
  // a[13]="219o";
  // a[14]="219p";
  // a[15]="216g";
  // a[16]="216f";

  int i;
  cmd = system("mkdir /tmp/QueriedData"); //make a directory in haoop2, where all operations happen here
  //forloop from 1-16 missing 17
  for (i = 0; i < nnodes; i++) {
    snprintf(slaveToMaster, 73, "scp -r 219a:/tmp/MyMonth/ /tmp/QueriedData", a[i]);
    snprintf(rmSlaveTmpData, 35, "ssh %s 'rm -r /tmp/MyMonth/'", a[i]);

    printf("HELLOO this is %s.... Moving data to Hadoop2\n", a[i]);
    cmd = system(slaveToMaster);    //copy data to Hadoop2

    printf("Removing from Slave......\n");
    cmd = system(rmSlaveTmpData);   //erase data from slave

    printf("Moving from Hadoop to HDFS......\n");
    cmd = system("hdfs dfs -put /tmp/Wiki_Data/WikiData ./Wiki_Data/");  //move data to HDFS

    printf("Removing Data from Hadoop......\n");
    cmd = system("rm -r /tmp/Wiki_Data/WikiData"); //erase data from master hadoop2

  }
  cmd = system("rm -r /tmp/Wiki_Data"); //delete this project directory from hadoop2
}

void workernode()
{
  // int* sub_size; // a buffer that receives the data
  // sub_size = malloc(3*sizeof(int));
  MPI_Status status;

  int cmd, start, end, i, j;

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
  // printf("Rank: %d: list looks like: %d, %d, %d\n ", me, recv_buff[0], start, end);
  cmd = system("mkdir /tmp/WikiData");  //Create WikiData directory
  //584
  //first run 17, second run 104 + 17
  for (i = start; i <= end; i++)
  {
    ptm->tm_year = 0163;
    ptm->tm_mon = 4;
    ptm->tm_mday = i;
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
    printf("I am Hadoop2 of rank: %d\n", me);
    Manger_DistributeWork()
  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (me != 0)
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
