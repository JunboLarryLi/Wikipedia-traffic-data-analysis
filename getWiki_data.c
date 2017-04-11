// Description: This file works for downloading the data from wikimedia traffic data by using MPI

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

char* concat(const char *s1, const char *s2)
{
    const int len1 = strlen(s1);
    const int len2 = strlen(s2);
    char *result = malloc(len1+len2+1);//+1 for the zero-terminator
    //in real code you would check for errors in malloc here
    memcpy(result, s1, len1);
    memcpy(result+len1, s2, len2+1);//+1 to copy the null-terminator
    return result;
}

int main() {
    char *listOfYears = {"2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016"};
    //account for leap years, months with 30/31/28/29 days
    char *listOfMonths = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    //char *listOfDays = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "10", "11", "12", "10", "11", "12", "10", "11", "12", "10", "11", "12", "10", "11", "12"}
    char *date = concat(listOfYears[1], listOfMonths[5]);
    //system("wget -nc --directory-prefix=./ https://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-01/pagecounts-20150101-010000.gz");
    //system("gzip -d ")

    printf("%s\n",date);

    return 0;
}
