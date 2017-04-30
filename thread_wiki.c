
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include "threadQuery.h"


pthread_mutex_t Tlock;      //declare lock
WorkQueue_t * WorkPool;
char * dir = NULL;
void queryWiki(char * input, char * output);

// Target function
void * dowork (void * voidtdata)
{
  //initialize structures
  char * fileName = NULL;
  Par_Data_t * mydata = (Par_Data_t*)voidtdata;
  //while there are jobs in Pool. Fetch the next availale job
  while (WorkPool->idx_txtAvailable < WorkPool->len)
  {
    pthread_mutex_lock(&Tlock);
    if (WorkPool->idx_txtAvailable < WorkPool->len )
    {
      fileName = WorkPool->Queue[WorkPool->idx_txtAvailable];
      strtok(fileName, "\n");
      mydata->flag = 1;
      WorkPool->idx_txtAvailable +=1;
    }
    pthread_mutex_unlock(&Tlock);
    if(mydata->flag == 1)
    {
      char * OutFile = (char*)malloc(40);
      char * inputFile = (char*)malloc(35);
      //Create output path and output file in tmp locally
      snprintf(OutFile, 40, "/tmp/MyMonth_out/%s/%s", dir, fileName); //touch /tmp/MyMonth_out/2015-02/part-r-00000
      //creating appropiate input path into tmp
      snprintf(inputFile, 35, "/tmp/MyMonth/%s/%s", dir, fileName); ///tmp/MyMonth/2015-02/part-r-00000

      queryWiki(inputFile, OutFile);
    }
    mydata->flag = 0;
  }
}

// Parallel Helper --> initializer can callee
void GetCategoriesParallel (int p, char * dir)
{
  // Declaration and initialization of variables
  int i, j, n, rc, node;
  char locatioOfDirNames[40]; //
  snprintf(locatioOfDirNames, 40, "/tmp/MyMonth/%s/NameOfFiles.txt", dir); ///tmp/MyMonth/2015-05/NameOfFiles.txt
  char line[13];
  FILE * DirContent;
  DirContent = fopen(locatioOfDirNames, "r");
  if (DirContent == NULL) {
    printf("ERROR: No directory content file.\n");
    exit(-1);
  }
  int numFileInDir = 0;
  i = 0;
  while (fgets(line, sizeof(line), DirContent)) {
    WorkPool->Queue[i] = (char *)malloc(sizeof(line));
    for(j = 0; j < sizeof(line); j++) {
      WorkPool->Queue[i][j] = line[j];
    }
    strtok(WorkPool->Queue[i], "\n");
    numFileInDir += 1;
    i += 1;
  }
  fclose(DirContent);
  WorkPool->len = numFileInDir;
  WorkPool->idx_txtAvailable = 0;

  // Initialize heap arrays for PTHREADS
  pthread_t * Threads;
  Par_Data_t * Threa_Data;
  Threads = (pthread_t *)malloc(p * sizeof(pthread_t));
  Threa_Data = (Par_Data_t *)malloc(p * sizeof(Par_Data_t));
  // Initialize data for threads
  for (i = 0; i < p; i++) {
    Threa_Data[i].ID = i;
    Threa_Data[i].flag = 0;
  }
  pthread_mutex_init(&Tlock, NULL);
  for (i = 0; i < p; i++) {
    rc = pthread_create(&Threads[i], NULL, dowork, (void *)&Threa_Data[i]);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }

  for (int t = 0; t < p; t++) {
    pthread_join(Threads[t],NULL);
  }
  //============================================
  //==================Free Data=================
  free(Threa_Data);
  return;
}

extern void MultiThreadQuery(char * directory, int ThreadNum)
{
  int n = 60;
  int nthreads = ThreadNum;
  dir = directory;
  WorkPool = (WorkQueue_t *)malloc( sizeof(WorkQueue_t) );
  WorkPool->Queue = (char * *)malloc((n-1) * sizeof(char *));
  WorkPool->idx_txtAvailable = -1;
  WorkPool->len = 0;
  GetCategoriesParallel(nthreads, dir);
  for (int i = 0; i < WorkPool->len; i++) {
    free(WorkPool->Queue[i]);
  }
  free(WorkPool);
  pthread_exit(NULL);
}
