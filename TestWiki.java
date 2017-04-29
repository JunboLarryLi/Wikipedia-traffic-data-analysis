//java -jar target/testwiki-1.0-SNAPSHOT.jar

package edu.denison.cs345.testwiki;

import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.Object;
// import org.json.JSONObject;
import java.lang.String;

/**
 * Hello world!
 * java -jar target/testwiki-1.0-SNAPSHOT.jar
 */
public class TestWiki
{
    public static void main( String[] args )
    {
        // The name of the file to open
        String fileName = "part-r-00017";

        // This will reference one line at a time
        String line = null;

        try
        {
            //FileReader reads text files in the default encoding
            FileReader fileReader = new FileReader(fileName);

            // ALways wrap FileReader in BufferedReader
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            int index = 1;
            while ((line = bufferedReader.readLine()) != null)
            {
              System.out.println(index);
              // System.out.println(line);
              // String line = "1502s20-09	en Google 2 151515";
              // line = value.toString();           //Convert a line to string
              //====================== Split information in a line by whitespace characters into a list ============================
              String name, views, dateHR, bytes, LineList[], Line[];

              Line = line.trim().split("\\t");  //splits the line by spaces and solve issues if there are many spaces
              dateHR = Line[0];

              LineList = Line[1].trim().split("\\s");
              name = LineList[1];//
              views = LineList[2];
              bytes = LineList[3];
              // More work will be done iff the language encoding is  en, En, En.d, and en.n
              /*
               * action = query
               * format = json          --> Output Format
               * prop = categories      --> desire proterty
               * title = name           --> from the line in the input file
               * clshow = !hidden       --> public categories
               * cllimit = 5            --> only the first 5 categories
               */
              //in a line of the input file we extract the name:
              String sentence, cat, _cat, page_id, title_idx, entry, _value, urlCmd, notHidden;
              sentence = "";
              cat = "<categories>";
              _cat = "</categories>";
              page_id = "<page _idx=\"";
              title_idx = "title=\"Category:";
              //CONSTANT String the indicates the entry of the API
              notHidden = "&clshow=!hidden&cllimit=1";

              try {
                 //Complete URL that indicates the action we need from the WikiMedia DB:
                 URL url = new URL("https://en.wikipedia.org//w/api.php?action=query&format=xml&prop=categories&titles=" + name + notHidden);
                 Scanner s = new Scanner(url.openStream());
                 s.useDelimiter("\\n");
                 sentence = s.next();
                 //========== index of the start and end of the page index =============
                 int s_indx, e_indx;
                 s_indx = sentence.indexOf(page_id);

                 if (s_indx != -1) // check if the XML contains page _idx.
                 {
                   s_indx += page_id.length();
                   e_indx = sentence.indexOf("\"", s_indx);
                   String idx = sentence.substring(s_indx, e_indx);
                   //  int indexNumber = Integer.parseInt(try_it);
                   String negtive_one = "-1";
                   if (!idx.equals(negtive_one))   // check if page _idx != -1 --> if it is a valid page
                   {
                     //========== index of the start and end of the Category List ==========
                     int s_cat, e_cat;
                     s_cat = sentence.lastIndexOf(cat);
                     if (s_cat != -1)     // checks whether the valid page has categories
                     {
                       s_cat += cat.length();
                       e_cat = sentence.indexOf(_cat);

                       if (e_cat < sentence.length())
                       {
                         String categories = sentence.substring(s_cat, e_cat);
                         //====================== Split information in a line by whitespace characters into a list ============================
                         String categoryList = "";
                         int s_title = 1;
                         int e_title = 1;
                         int flag = 0;
                         while(flag != -1) // Iterates through the categories --> if it has less than 5 jumps out.
                         {
                           flag = categories.indexOf(title_idx, e_title);
                           if (flag != -1) {
                             s_title = flag + title_idx.length();
                             e_title = categories.indexOf("\"", s_title);
                             String category = categories.substring(s_title, e_title);
                             categoryList += (category + "\t");
                           }
                         }

                        //  _value = categoryList + views + "\t" + bytes;

                         //  set KET and VALUE
                        //  KEY.set(dateHR);
                        //  VALUE.set(_value);
                        //  //COMMIT KET and VALUE
                        //  context.write(KEY, VALUE);
                        _value = "name: " + name + " --- categories: " + categoryList;
                        System.out.println(_value);
                       }
                     }
                   }
                 }
                index += 1;
                // KEY.set(dateHR);
                // VALUE.set(idx);
                //
                // //COMMIT KET and VALUE
                // context.write(KEY, VALUE);

              }
              catch(IOException ex) {
                 ex.printStackTrace(); // for now, simply output it.
              }
            }
        }
        catch(FileNotFoundException ex)
        {
          System.out.println("Unable to open file '" + fileName + "'");
        }
        catch(IOException ex)
        {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }



    }
}

