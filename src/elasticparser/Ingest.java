/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package elasticparser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ivan
 */
class Ingest {
    
    public Ingest(){
        createIndex("csfd");
        createIndex("mall");
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/csfd/negative.txt", "csfd", "negative");
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/csfd/positive.txt", "csfd", "positive");
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/csfd/neutral.txt", "csfd","neutral");
        
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/mallcz/negative.txt", "mall", "negative");
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/mallcz/positive.txt", "mall", "positive");
        loadFile("C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data/mallcz/neutral.txt", "mall", "neutral");
        
        //loadFile("negative.txt", "mall", "negative");
    }

    private void loadFile(String soubor, String index, String type) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(soubor));
            String line = br.readLine();
            int i = 1;
            while (line != null){
                URL url = new URL("http://es.vse.cz:9200/" + index +"/"+ type +"/" + i);
                HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                httpCon.setDoOutput(true);
                httpCon.setRequestMethod("PUT");
                OutputStreamWriter out = new OutputStreamWriter(
                httpCon.getOutputStream());
                line = line.replaceAll("\\\\", "");
                line = "{ \"body\" : \"" +line.replaceAll("\"", "") + "\"}";
              //  System.out.println(line);
                out.write(line);
                out.close();

                 BufferedReader in = new BufferedReader(
		        new InputStreamReader(httpCon.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
                
                
                line = br.readLine();
                i++;

            }
                    
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    private void createIndex(String index){
         try {
            URL url = new URL("http://es.vse.cz:9200/" + index + "/");
            HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
            httpCon.setDoOutput(true);
            httpCon.setRequestMethod("POST");
            
           BufferedReader in = new BufferedReader(
		        new InputStreamReader(httpCon.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
            
            
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
