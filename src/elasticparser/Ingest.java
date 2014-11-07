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
 * @author Ivan Jelinek
 */
class Ingest {

    private String hostES = "localhost";
    private String portES = "9200";
    private String pathToData = "C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data";

    public Ingest() {
        createIndex("csfd");
        createIndex("mall");

        prepareMapping("csfd", "negative");
        prepareMapping("csfd", "neutral");
        prepareMapping("csfd", "positive");
        prepareMapping("mall", "negative");
        prepareMapping("mall", "neutral");
        prepareMapping("mall", "positive");

        loadFile(pathToData + "/csfd/negative.txt", "csfd", "negative");
        loadFile(pathToData + "/csfd/positive.txt", "csfd", "positive");
        loadFile(pathToData + "/csfd/neutral.txt", "csfd", "neutral");

        loadFile(pathToData + "/mallcz/negative.txt", "mall", "negative");
        loadFile(pathToData + "/mallcz/positive.txt", "mall", "positive");
        loadFile(pathToData + "/mallcz/neutral.txt", "mall", "neutral");
    }

    private void loadFile(String soubor, String index, String type) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(soubor));
            String line = br.readLine();
            int i = 1;
            while (line != null) {
                line = line.replaceAll("\\\\", "");
                line = "{ \"body\" : \"" + line.replaceAll("\"", "") + "\"}";

                URL url = new URL("http://" + hostES + ":" + portES + "/" + index + "/" + type + "/" + i);
                System.out.println(sendRQ(url, "PUT", line));
                /*  HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                 httpCon.setDoOutput(true);
                 httpCon.setRequestMethod("PUT");
                 OutputStreamWriter out = new OutputStreamWriter(
                 httpCon.getOutputStream());

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
                 */
                line = br.readLine();
                i++;

            }

        } catch (FileNotFoundException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void createIndex(String index) {
        try {
            String analyzer = "{\"settings\": {\"analysis\": {\"filter\": {\"czech_stop\": {\"type\":       \"stop\",\"stopwords\":  \"_czech_\"},\"czech_keywords\": {\"type\":       \"keyword_marker\",\"keywords\":   [\"x\"]}, \"czech_stemmer\": { \"type\":       \"stemmer\", \"language\":   \"czech\"}},\"analyzer\": {\"czech\": {\"tokenizer\":  \"standard\",\"filter\": [ \"lowercase\",\"czech_stop\", \"czech_keywords\", \"czech_stemmer\"]}}}}}";
            URL url = new URL("http://" + hostES + ":" + portES + "/" + index + "/");
            System.out.println(sendRQ(url, "PUT", analyzer));
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    private void prepareMapping(String index, String typ) {
        String mappingBody = "{\"properties\": {\"czech\": {\"type\":\"string\",\"analyzer\": \"czech\"}}}";
        try {
            System.out.println(sendRQ(new URL("http://" + hostES + ":" + portES+"/" + index + "/_mapping/" + typ), "POST", mappingBody));
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private String sendRQ(String hostES, String portES, String index, String typ, String method, String message) {

        String urlString = "";
        try {
            if (typ == null) {
                urlString = "http://" + hostES + ":" + portES + "/" + index + "/";
            } else {
                urlString = "http://" + hostES + ":" + portES + "/" + index + "/" + typ;
            }
            URL url = new URL(urlString);
            return sendRQ(url, method, message);

        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    private String sendRQ(URL url, String method) {
        return sendRQ(url, method, "");
    }

    private String sendRQ(URL url, String method, String message) {
        try {
            HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
            httpCon.setDoOutput(true);
            httpCon.setRequestMethod(method);

            if (!method.equals("GET")) {
                OutputStreamWriter out = new OutputStreamWriter(
                        httpCon.getOutputStream());
                out.write(message);
                out.close();
            }

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(httpCon.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            //print result
            return response.toString();
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;

    }
}
