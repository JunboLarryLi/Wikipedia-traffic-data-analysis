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

void init(int argc, char **argv)
{
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nnodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
}

void managernode_DistWork()
{
  MPI_Status status;
  int i;

  //dividing data among all the nodes except managernode
  //sub_size[0] = size
  //sub_size[1] = start number
  //sub_size[1] = end number
  // int* sub_size;
  // sub_size = malloc(3*sizeof(int));
  int sub_size[3];
  sub_size[0] = 584/(nnodes-1);
  sub_size[1] = 0;
  sub_size[2] = 0;

  // int* last_sub_size;
  // last_sub_size = malloc(3*sizeof(int));
  int last_sub_size[3];
  last_sub_size[0] = sub_size[0] + (584 - sub_size[0]*(nnodes-1));
  // printf("caooooooooooooooo %d\n", last_sub_size[0]);
  last_sub_size[1] = 0;
  last_sub_size[2] = 0;
  // printf("Rank: %d: list looks like: %d, %d, %d \n", me, sub_size[0], sub_size[1], sub_size[2]);
  // send share of the downloading dates to worker nodes
  for (i = 1; i < nnodes-1; i++)
  {
    sub_size[1] = (i-1)*sub_size[0]+1;
    sub_size[2] = i*sub_size[0];
    // printf("Rank: %d: list looks like: %d, %d, %d \n", me, sub_size[0], sub_size[1], sub_size[2]);
    MPI_Send (&sub_size, 3, MPI_INT, i, DATA_MSG, MPI_COMM_WORLD);
  }
  last_sub_size[1] = (i-1)*sub_size[0]+1;
  last_sub_size[2] = last_sub_size[1] + last_sub_size[0]-1;
  // printf("Rank: %d: list looks like: %d, %d, %d \n", me, last_sub_size[0], last_sub_size[1], last_sub_size[2]);
  MPI_Send (&last_sub_size, 3, MPI_INT, i, DATA_MSG, MPI_COMM_WORLD);
}

void workernode()
{
  // int* sub_size; // a buffer that receives the data
  // sub_size = malloc(3*sizeof(int));
  MPI_Status status;
  int cmd;
  //char file[30];
  //reeive message from manager node
  MPI_Recv(&recv_buff, 3, MPI_INT, 0, DATA_MSG, MPI_COMM_WORLD, &status);
  printf("Rank: %d: list looks like: %d, %d, %d\n ", me, recv_buff[0], recv_buff[1], recv_buff[2]);
  if (me == 2) {
    system("wget https://dumps.wikimedia.org/other/pagecounts-raw/2007/2007-12/pagecounts-20071210-200000.gz -P /tmp/WikiData/");

  }
  if (me == 3) {
    system("wget https://dumps.wikimedia.org/other/pagecounts-raw/2007/2007-12/pagecounts-20071210-210000.gz -P /tmp/WikiData/");
  }
  //do work
  /*
  write
  code
  from
  here
  */
  // float sub_pi = calculate_pi(sub_size);

  //send result back to manager node
  // MPI_Send(&sub_pi, 1, MPI_INT, 0, NEWDATA_MSG, MPI_COMM_WORLD);
}

int main(int argc, char** argv)
{
  int i;
  init(argc, argv);
  // printf("rank: %d\n", me);
  if (me == 0)
  {
    printf("rankkkkkkkkkkkkk: %d\n", me);
    managernode_DistWork();
  }
  else
  {
    workernode();
  }
  MPI_Finalize();
}

/*
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

    char MakeDir[28];
    char wgetCommand[122];
    //char unZipCommand[114];
    //char RemoveCommand[114];

    int start = 2;
    int end = 3;

    system("mkdir /tmp/WikiData");  //Create WikiData directory
    //583 --> since we r doing inclsive bound
    for (int i = start; i <= end; i++)
    {
      ptm->tm_year = 0163;
      ptm->tm_mon = 0;
      ptm->tm_mday = i;
      mktime(ptm);
      strftime(date, 9, "%Y%m%d", ptm);
      memcpy(&year, &date[0], 4);
      year[4] = '\0';
      memcpy(&month, &date[4], 2);
      month[2] = '\0';
      snprintf(URL_forDay, 82, "https://dumps.wikimedia.org/other/pagecounts-raw/%s/%s-%s/pagecounts-%s", year, year, month, date);
      snprintf(MakeDir, 28, "mkdir /tmp/WikiData/%s-%s", year, month);

      system(MakeDir);

      printf("%s\n", URL_forDay);
      for (int j = 0; j < 5; j++) {
        if (j < 10) {
          snprintf(Download_gz_URL, 100, "%s-0%d0000.gz", URL_forDay, j);
        }
        else {
          snprintf(Download_gz_URL, 100, "%s-%d0000.gz", URL_forDay, j);
        }
        memcpy(&nameOfGZ, &Download_gz_URL[62], 26);
        nameOfGZ[26] = '\0';  //get name of the

        snprintf(wgetCommand, 154, "wget %s -P /tmp/WikiData/%s-%s", Download_gz_URL, year, month);
        system(wgetCommand);

      }


      //for slave nodes to run HADOOP DOESN'T RUN THESE COMMANDS
      //snprintf(unZipCommand, 114, "hadoop fs -text ./WikiData/%s.gz | hadoop fs -put - ./WikiData/%s", nameOfGZ, nameOfGZ);
      //system(unZipCommand);
    }
    system("scp -r /tmp/WikiData/ valdiv_n1@hadoop2.mathsci.denison.edu:/users/valdiv_n1/CS345/Hadoop/Wiki_Data");
    //system("rm -r /tmp/WikiData/")
    //MASTER in a separete forloop
    // if rank == 0
    // system("hdfs dfs -put ~/CS345/Hadoop/Wiki_Data/ .")
    return 0;
}

*/
