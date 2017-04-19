/*
 Description:
 In this file, we mainly calculated the percentage of party-line for each year.

 In stage 1:
 mapper <year.rollNum, PartyVote>   --->>    Reducer <year.rollNum, if_partyline>

 In stage 2:
 mapper <year, if_partyline>    --->>  Reducer <year, percentage>

 We can see two stages' outputs from:
 http://w01:50070/explorer.html#/user/li_j8/PartyLine

 Run:
 hadoop jar target/partyline-1.0-SNAPSHOT.jar /user/valdiv_n1/partyLine/* ./PartyLine/stage1_output ./PartyLine/stage2_output
 */

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
         *  < KEY, VALUE> OUTPUT FORMAT 1
         * KEY:   YY.MM.DD-HH
         * VALUE: LANG¿NAME¿VIEWS¿BYTES.

         *  < KEY, VALUE> OUTPUT FORMAT 2
         * KEY:   YYMMDD-HH
         * VALUE: LANG'\t'NAME'\t'VIEWS'\t'BYTES.
         ********************************************************************************************/

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line, vLine_1, vLine_2, vLine_3, vLine_4;
            FileSplit fileSplit;
            Path pathRaw;

            //Valid language encoders===============
            vLine_1 = "en";
            vLine_2 = "En";
            vLine_3 = "En.d";
            vLine_4 = "en.n";
            //======================================

            fileSplit= (FileSplit)context.getInputSplit();
            pathRaw = fileSplit.getPath();     // Get the path

            line = value.toString();           //Convert a line to string

            //====================== Split information in a line by whitespace characters into a list ============================
            String lang, Line[];

            Line = line.trim().split("\\s");  //splits the line by spaces and solve issues if there are many spaces
            lang = Line[0];    //get language encoding of the line

            // More work will be done iff the language encoding is  en, En, En.d, and en.n
            if ( lang.equals(vLine_1) || lang.equals(vLine_2) || lang.equals(vLine_3) || lang.equals(vLine_4) )
            {
                String pathL[], nameOfFile, _key, yy, mm , dd, hh;
                /*=========================================================
                ======================= MAKE KEY ========================*/
                // I.
                //  0/  1 /  2  /  3 /   4   /  5
                //   /user/li_j8/Wiki/2015-01/
                // II.
                //  0/  1 /    2    /     3  / 4   /  5 /   6   /  7
                //   /user/valdiv_n1/TESTWIKI/INPUT/Wiki/2015-01/
                // Get the date and time of the file to which the line balongs.
                pathL = (pathRaw.toString()).split("/");
                nameOfFile = pathL[7];  //name pagecounts-DATE-TIME from II.
                //============ way 1:  YY.MM.DD-HH =======================
                yy = nameOfFile.substring(13,15); // year (last 2 digits)
                mm = nameOfFile.substring(15,17); // month
                dd = nameOfFile.substring(17,19); // day
                hh = nameOfFile.substring(20,22); // time (just hours)
                _key = yy +"."+ mm +"."+ dd + "-"+ hh;
                //============== way 2:  YYMMDD-HH ========================
                String date_time = nameOfFile.substring(13,22);
                _key = date_time;

                /*==========================================================
                ====================== MAKE VALUE ========================*/
                String name, views, bytes, _value;
                name = Line[1];
                views = Line[2];
                bytes = Line[3];

                _value = lang + "¿" + name + "¿" + views + "¿" + bytes; // WAY 1

                _value = lang + "\t" + name + "\t" + views + "\t" + bytes; // WAY 2

                KEY.set(_key);
                VALUE.set(_value);
                context.write(KEY, VALUE);
            }
        }
    }

    public static class Reduce_1 extends Reducer<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

            // The following 4 variables are used to track the count of each outcomes
            int Rep_y = 0;
            int Rep_n = 0;
            int Dem_y = 0;
            int Dem_n = 0;
            for (Text val : values)
            {
                String V = val.toString();
                // The party and vote here matches the value that is from mapper
                String party = V.substring(0,1);
                String vote = V.substring(1,2);
                if (party.equals("R"))
                {
                    if (vote.equals("Y"))
                    {
                        Rep_y += 1;
                    }
                    else
                    {
                        Rep_n += 1;
                    }
                }
                else if (party.equals("D"))
                {
                    if (vote.equals("Y"))
                    {
                        Dem_y += 1;
                    }
                    else
                    {
                        Dem_n += 1;
                    }
                }
            }
            // Calculate the majority vote for each party
            int Rep = Rep_y - Rep_n;
            int Dem = Dem_y - Dem_n;

            // If the following condition is true, then it is a party line; Otherwise it is not.
            if ((Rep < 0 && Dem > 0) || (Rep > 0 && Dem < 0))
            {
                context.write(key, one);
            }
            else
            {
                context.write(key, zero);
            }
        }
    }

    public static class Map_2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text KEY = new Text();
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line, year, val;
            line = value.toString();
            year = line.substring(0,4);
            int tabIndx = line.indexOf("\t");
            val = line.substring(tabIndx+1, tabIndx+2); // Get the 0 or 1 from each year's roll call.
            KEY.set(year);
            if (val.equals("1"))
            {
                context.write(KEY, one);
            }
            else
            {
                context.write(KEY, zero);
            }
        }
    }

    public static class Reduce_2 extends Reducer<Text, IntWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int PL = 0;
            int len = 0;
            for (IntWritable val : values)
            {
                PL += val.get(); // Get the sum of the yes
                len += 1; // Get the total count of votes
            }
            float percent = (((float)PL/len) * 100);
            context.write(key, new FloatWritable(percent));
        }

    }
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Job 1 is charge of the first stage
    Job job = new Job(conf, "partyline");
    job.setJarByClass(PartyLine.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map_1.class);
    job.setReducerClass(Reduce_1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    Path OUTPUT_DIR = new Path(args[1]); // Used for the following second stage's input path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);

    job.waitForCompletion(true); // Can't start until the first stage is finished

    // Job 2 is charge of the first stage
    Job job2 = new Job(conf, "partyline2");
    job2.setJarByClass(PartyLine.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    job2.setMapperClass(Map_2.class);
    job2.setReducerClass(Reduce_2.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job2, OUTPUT_DIR);
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.waitForCompletion(true);

}

}
