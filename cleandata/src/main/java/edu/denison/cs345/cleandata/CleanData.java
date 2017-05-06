/*
 Description:
          In this file, we wrote a simpe mapreduce job to get rid of the
          rows that are non-english rows.
 */

// Compile instructions:
// There is a script to run the job. The scipt is located at the first level of the directory, named as cleanDataScript.


package edu.denison.cs345.cleandata;

import java.io.IOException;
import java.util.*;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CleanData {
    // **note: Files' content are white character separated. use \w in the regEx**
    public static class Map_1 extends Mapper<LongWritable, Text, Text, Text> {
        //private final static FloatWritable one = new IntWritable(1);
        private Text KEY = new Text();
        private Text VALUE = new Text();
        /********************************************************************************************
         *  < KEY, VALUE> OUTPUT FORMAT
         * KEY:   YYMMDD-HH
         * VALUE: LANG NAME VIEWS BYTES.
         ********************************************************************************************/
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line, vLine_1, vLine_2, vLine_3, vLine_4;

            //Valid language encoders===============
            vLine_1 = "en";
            vLine_2 = "En";
            vLine_3 = "En.d";
            vLine_4 = "en.n";
            //======================================

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            Path pathRaw = fileSplit.getPath();     // Get the path

            line = value.toString();           //Convert a line to string

            //====================== Split information in a line by whitespace characters into a list ============================
            String lang, Line[];

            Line = line.trim().split("\\s");  //splits the line by spaces and solve issues if there are many spaces
            lang = Line[0];    //get language encoding of the line

            // More work will be done iff the language encoding is  en, En, En.d, and en.n
            if ( lang.equals(vLine_1) || lang.equals(vLine_2) || lang.equals(vLine_3) || lang.equals(vLine_4) )
            {
                /*=========================================================
                ======================= MAKE KEY ========================*/

                String pathL[], nameOfFile, _key, date_time;
                // Get the date and time of the file to which the line balongs.
                pathL = (pathRaw.toString()).split("/");
                // I. REAL
                //  0/  1 /  2  /  3 /   4   /  5
                //   /user/li_j8/Wiki/2015-01/pagecounts-2015010*-TIME
                // II.  TESTING
                //  0/  1 /    2    /     3  / 4   /  5 /   6   /  7
                //   /user/valdiv_n1/TESTWIKI/TRY  / page
                //   /user/valdiv_n1/TESTWIKI/INPUT/Wiki/2015-01/pagecounts-DATE-TIME.tt
                //nameOfFile = ;  //name pagecounts-DATE-TIME from II.
                //============== way 2:  YYMMDD-HH ========================
                date_time = pathL[7].substring(13,22); //Before "/user", there should be two more "/". Maybe it is "hdfs://"
                KEY.set(date_time);
                //_key = ;
                /*==========================================================
                ====================== MAKE VALUE ========================*/
                String _value;
                _value = line;
                //set KET and VALUE
                VALUE.set(_value);
                //COMMIT KET and VALUE
                context.write(KEY, VALUE);
            }
        }
    }

    public static class Reduce_1 extends Reducer<Text, Text, Text, Text> {
        private Text VALUE = new Text();
        //private final static IntWritable one = new IntWritable(1);
        //private final static IntWritable zero = new IntWritable(0);
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

            for (Text val : values)
            {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "cleandata");
    job.setJarByClass(CleanData.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map_1.class);
    job.setReducerClass(Reduce_1.class);
    job.setNumReduceTasks(32);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true); // Can't start until the first stage is finished

}

}
