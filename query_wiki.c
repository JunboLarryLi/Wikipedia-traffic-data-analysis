#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

int cmd;

void renameFiles(char * fileName)
{
    FILE * fp;
    char line[10000];
    fp = fopen(fileName, "r");
    int i;
    int index = 1;
    printf("\n");

    while (fgets(line, sizeof(line), fp))
    {
      i = 0;
      printf("index: %d ", index);
      //print the line
      while (line[i] != '\n')
      {
        printf("%c", line[i]);
        i+=1;
      }

      printf("\n");
      index += 1;
    }
    fclose(fp);
}

int main(int argc, char** argv)
{
  if (argc != 2) {
    exit(-1);
  } else {
  renameFiles(argv[1]);
  }
  return 0;
}








// ~/Downloads/viper
