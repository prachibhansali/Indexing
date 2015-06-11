package com.ir.indexing.tokenizer;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class QueryParser {

	static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StemmerStopwords/";
	public static void main(String[] args) throws Exception {
		performParsing();
		
	}
	
	private static void performParsing() throws Exception {
		RandomAccessFile f = new RandomAccessFile(docroot+"index.txt","rw");
		HashMap<String,Integer> terms = new HashMap<String,Integer>();
		HashMap<Integer,String> t = new HashMap<Integer,String>();
		HashMap<Integer,String> docs = new HashMap<Integer,String>();
		HashMap<String,HashMap<Integer,Float>> tfindex =  new HashMap<String,HashMap<Integer,Float>>();
		HashMap<Integer, Integer> doclengths = new HashMap<Integer,Integer>();
		
		long totaldocs = docs.size();
		
		Catalog c = new Catalog();
		c.loadFromFile(docroot+"catalog.txt");
		loadFromFile(terms,docs,t);

		HashMap<Integer,Integer> docFreq = new HashMap<Integer,Integer>();
		double avglength = (fetchDocFreqOfAllTerms(f,c,docFreq,doclengths,t))/totaldocs;
		
		// Fetch Vocabulary Count of corpus
		long vocabcount = c.size();
		
		// Fetching stopwords
		HashMap<String,Integer> stopwords = new HashMap<String,Integer>();
		extractStopwords(stopwords);
		
		//Start Query Parsing 
		HashMap<Integer,ArrayList<String>> queryKeywords = new  HashMap<Integer,ArrayList<String>>();
		ArrayList<Integer> queryId = new ArrayList<Integer>();

		File file = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/query_desc.51-100.short.txt");
		//File file = new File("C:\\Users\\Bhansali\\workspace\\ElasticSearch\\query.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String query;
		
	}

	private static void extractStopwords(HashMap<String,Integer> h) throws IOException
	{
		System.out.println("****Fetching stop words*****");
		BufferedReader b = new BufferedReader(new FileReader(docroot+"stoplist.txt"));
		String stopword="";
		while((stopword=b.readLine())!=null)
			h.put(stopword.toLowerCase().trim(), 0);
		b.close();
	}
	
	private static void loadFromFile(HashMap<String, Integer> terms,
			HashMap<Integer, String> docs,HashMap<Integer,String> t) throws Exception 
	{
		BufferedReader br = new BufferedReader(new FileReader(docroot+"Term-ID-Mappings"));
		String line = "";
		while((line=br.readLine())!=null)
			{
			terms.put(line.split(" ")[0], Integer.parseInt(line.split(" ")[1]));
			t.put(Integer.parseInt(line.split(" ")[1]),line.split(" ")[0]);
			}
		br.close();
		br = new BufferedReader(new FileReader(docroot+"Doc-ID-Mappings"));
		line = "";
		while((line=br.readLine())!=null)
			docs.put(Integer.parseInt(line.split(" ")[0]),line.split(" ")[1]);
		br.close();
	}

	private static long fetchDocFreqOfAllTerms(RandomAccessFile f, Catalog c, 
			HashMap<Integer, Integer> docFreq, HashMap<Integer, Integer> doclengths,HashMap<Integer, String> t) throws IOException {
		Iterator keys = c.getTermKeySet().iterator();
		int totallength=0;
		//PrintWriter pw = new PrintWriter("outputs");
		while(keys.hasNext())
		{
			int id = (int)keys.next();
			String terminfo = fetchForIDFromIndex(f,c.get(id));
			//System.out.println(id+" " +terminfo.substring(0, 10));
			//docFreq.put(id,countDocuments(line));
			Pattern matchdocs = Pattern.compile("(\\$([^\\$]+))");
			Matcher m = matchdocs.matcher(terminfo);
			int numOfDocs=0;
			int docID = -1;
			while(m.find())
			{
				numOfDocs++;
				String docinfo = m.group(2);
				docID = Integer.parseInt(docinfo.substring(0, docinfo.indexOf("*")));
				int count = countTF(docinfo);
				int initlength = doclengths.containsKey(docID) ? doclengths.get(docID) : 0;
				doclengths.put(docID, initlength+count);
				totallength++;
			}
			if(docID!=-1) docFreq.put(docID, numOfDocs);
		}
		return totallength;
	}
	
	private static int countDocuments(String line) {
		int count=0;
		for(int i=0;i<line.length();i++)
			if(line.charAt(i)=='$') count++;
		return count;
	}
	
	private static String fetchForIDFromIndex(RandomAccessFile f, Tuple tuple) throws IOException {
		byte b[] = new byte[(int) tuple.getSize()];
		f.seek((int) tuple.getOffset());
		f.read(b);
		return new String(b);
	}

	private static HashMap<Integer, Float> retrievePostings(RandomAccessFile f,Catalog c,int term, HashMap<String, Long> docFreq) 
			throws IOException 
	{
		HashMap<Integer,Float> termFreq = new HashMap<Integer,Float>();
		if(!c.contains(term)) return termFreq;
		else {
			String terminfo = fetchForIDFromIndex(f,c.get(term));
			Pattern matchdocs = Pattern.compile("(\\$([^\\$]+)");
			Matcher m = matchdocs.matcher(terminfo);
			while(m.find())
			{
				String docinfo = m.group(2);
				int docID = Integer.parseInt(docinfo.substring(0, docinfo.indexOf("*")));
				int count = countTF(docinfo);
				termFreq.put(docID, (float)count);
			}
		}
		return termFreq;
	}
	
	private static int countTF(String docinfo) {
		int count =0;
		for(int i=0;i<docinfo.length();i++)
			if(docinfo.charAt(i)=='*') count++;
		return count;
	}

	private static void print(HashMap<Integer, Integer> docFreq) {
		Iterator keys = docFreq.keySet().iterator();
		while(keys.hasNext())
		{
			int id = (int)keys.next();
			System.out.print(id+" "+docFreq.get(id) +" ");
		}
	}
	
	
}
