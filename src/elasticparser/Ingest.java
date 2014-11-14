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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 * Trida pro zpracovani dat - index CSFD a MALL
 * Urceno pro studenty VŠE v Praze - Kompetencniho centra pro nestrukturovana data
 * 
 * @author Ivan Jelinek
 */
class Ingest {

    //kde mate spusten ES
    private String hostES = "localhost";
    private String portES = "9200";
    // upravte si cestu k datovym souborum
    private String pathToData = "C:\\Users\\jelineiv\\Dropbox\\The Analytical Company\\data";

    public Ingest() {
        //vytvori indexy a priradi jim analyzery
        createIndex("csfd");
        createIndex("mall");

        //vytvori mapovani pro defaultni analyzery
        prepareMapping("csfd", "negative");
        prepareMapping("csfd", "neutral");
        prepareMapping("csfd", "positive");
        prepareMapping("mall", "negative");
        prepareMapping("mall", "neutral");
        prepareMapping("mall", "positive");

        //zpracuje data
        loadFile(pathToData + "/csfd/negative.txt", "csfd", "negative");
        loadFile(pathToData + "/csfd/positive.txt", "csfd", "positive");
        loadFile(pathToData + "/csfd/neutral.txt", "csfd", "neutral");

        loadFile(pathToData + "/mallcz/negative.txt", "mall", "negative");
        loadFile(pathToData + "/mallcz/positive.txt", "mall", "positive");
        loadFile(pathToData + "/mallcz/neutral.txt", "mall", "neutral");
    }

    /**
     * Metoda nacte soubory a radek po radku je rozparsuje do zadaneho indexu a typu
     * 
     * @param soubor cesta k souboru
     * @param index index v ES
     * @param type typ v ES
     */
    private void loadFile(String soubor, String index, String type) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(soubor));
            String line = br.readLine();
            int i = 1;
            
            Random randomGenerator = new Random();
            
            while (line != null) {
               // line = line.replaceAll("\\\\", "");
               // line = "{ \"body\" : \"" + line.replaceAll("\"", "") + "\"}";
                JSONObject lineJS = new JSONObject();
                lineJS.put("body", line);
                lineJS.put("user", randomGenerator.nextInt(5000));
                if (i <= 1000){
                    lineJS.put("segment", "A");
                }
                if (i <= 3000 && i > 1000){
                    lineJS.put("segment", "B");
                } else {
                    lineJS.put("segment", "C");
                }
                URL url = new URL("http://" + hostES + ":" + portES + "/" + index + "/" + type + "/" + i);
                System.out.println(sendRQ(url, "PUT", lineJS.toString()));
                line = br.readLine();
                i++;

            }

        } catch (FileNotFoundException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Metoda vytvori index v ES
     * 
     * @param index nazev indexu, ktery ma byt vytvoren
     */
    private void createIndex(String index) {
        try {
            String analyzer = "{\"settings\": {\"analysis\": {\"filter\": {\"czech_stop\": {\"type\":       \"stop\",\"stopwords\":  \"_czech_\"},\"czech_keywords\": {\"type\":       \"keyword_marker\",\"keywords\":   [\"x\"]}, \"czech_stemmer\": { \"type\":       \"stemmer\", \"language\":   \"czech\"}},\"analyzer\": {\"czech\": {\"tokenizer\":  \"standard\",\"filter\": [ \"lowercase\",\"czech_stop\", \"czech_keywords\", \"czech_stemmer\"]}}}}}";
            URL url = new URL("http://" + hostES + ":" + portES + "/" + index + "/");
            System.out.println(sendRQ(url, "PUT", analyzer));
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Metoda vytvori mapovani pro analyzer k indexu a typu, ktery je specifikovan
     * 
     * @param index mapovani, ktere ma byt vytvoren
     * @param typ k namapovani
     */
    private void prepareMapping(String index, String typ) {
        //String mappingBody = "{\"properties\": {\"czech\": {\"type\":\"string\",\"analyzer\": \"czech\"}}}";
        JSONObject child = new JSONObject();
        child.put("type", "string");
        child.put("analyzer", "czech");
        JSONObject czech = new JSONObject();
        czech.put("czech", child);
        JSONObject mappingBody = new JSONObject();
        mappingBody.put("properties", czech);
        try {
            System.out.println(sendRQ(new URL("http://" + hostES + ":" + portES+"/" + index + "/_mapping/" + typ), "POST", mappingBody.toString()));
        } catch (MalformedURLException ex) {
            Logger.getLogger(Ingest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Metoda odesle RQ skrze REST API
     * 
     * @param hostES IP adresa ES
     * @param portES port ES
     * @param index index, kam se ma zprava poslat
     * @param typ typ kam se zprava posle
     * @param method GET, POST, PUT, DELETE
     * @param message JSON zprava
     * @return String odpoved ES nebo null pri chybe
     */
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
        } 
        return null;
    }

    /**
     * Pretizena verze metody pro odelsani pozadavku
     * 
     * @param url URL ES - cely string
     * @param method GET, POST, PUT, DELETE
     * @return String ES odpoved
     */
    private String sendRQ(URL url, String method) {
        return sendRQ(url, method, "");
    }

    /**
     * Pretizena verze metody, ktera skutecne odesle REST RQ
     * 
     * @param url URL string pro pripojeni k ES
     * @param method GET POST PUT DELETE
     * @param message JSON zprava
     * @return String odpoved ES null pri chybe
     */
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
