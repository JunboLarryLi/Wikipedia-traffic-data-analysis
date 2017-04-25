//java -jar target/testwiki-1.0-SNAPSHOT.jar

package edu.denison.cs345.testwiki;

import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.Object;
import org.json.JSONObject;
import java.lang.String;

/**
 * Hello world!
 * java -jar target/testwiki-1.0-SNAPSHOT.jar
 */
public class TestWiki
{
    public static void main( String[] args )
    {
      String line = "151220-08	en Yellow-breasted_antpitta 1 31990";
      //====================== Split information in a line by whitespace characters into a list ============================
      String name, Line[], dateHR, LineList[], views, bytes;

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
      String sentence, cat, _cat, page_id, title_idx, entry, _value;
      sentence = "";
      cat = "<categories>";
      _cat = "</categories>";
      page_id = "<page _idx=\"";
      title_idx = "title=\"Category:";
      //CONSTANT String the indicates the entry of the API
      entry = "https://en.wikipedia.org//w/api.php?";

      try {
         //Complete URL that indicates the action we need from the WikiMedia DB:
         URL url = new URL(entry + "action=query&format=xml&prop=categories&titles="+ name +"&clshow=!hidden&cllimit=5");
         Scanner s = new Scanner(url.openStream());
         s.useDelimiter("\\n");
         sentence = s.next();
         //========== index of the start and end of the page index =============
         int s_indx, e_indx;
         s_indx = sentence.indexOf(page_id) + page_id.length();
         e_indx = sentence.indexOf("\"", s_indx);
         String idx = sentence.substring(s_indx, e_indx);
         //========== index of the start and end of the Category List ==========
         int s_cat, e_cat;
         s_cat = sentence.lastIndexOf(cat) + cat.length();
         e_cat = sentence.indexOf(_cat);
         String categories = sentence.substring(s_cat, e_cat);
         //====================== Split information in a line by whitespace characters into a list ============================
         String categoryList = "";
         int s_title = 1;
         int e_title = 1;
         for (int i = 0; i < 5; i++) {
           s_title = categories.indexOf(title_idx, e_title) + title_idx.length();
           e_title = categories.indexOf("\"", s_title);
           String category = categories.substring(s_title, e_title);
           categoryList += (category + "\t");
         }

         _value = categoryList + views + "\t" + bytes;
         System.out.println(_value);
         System.out.println("\n");
         System.out.println(dateHR);
      }
      catch(IOException ex) {
         ex.printStackTrace(); // for now, simply output it.
      }
    }
}
