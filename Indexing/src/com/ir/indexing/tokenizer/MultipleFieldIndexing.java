package com.ir.indexing.tokenizer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ir.indexing.stemmer.Stemmer;

public class MultipleFieldIndexing {
	private static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StemmerStopwords/";
	static HashMap<Integer,String> wordEncodings = null;
	static HashMap<String,Integer> wordDecodings = null;
	static HashMap<String,String> decodings = null;
	static HashMap<String,String> encodings = null;

	public static void main(String[] args) throws Exception {
		/*PrintWriter out = new PrintWriter("temp1");
		HashMap<Integer,String> wordEncodings = new HashMap<Integer,String>();
		HashMap<String,Integer> wordDecodings = new HashMap<String,Integer>();
		createEncodings(wordEncodings,wordDecodings);
		//performEncoding("bhansali prachi sureshkumar, seema bhansali, anuja bhansali :*".getBytes(),wordEncodings);
		String encodedstr = performEncoding("bhansali",wordEncodings);
		System.out.println(encodedstr);
		out.println(new String(performDecoding(encodedstr,wordDecodings)));
		out.close();*/
		/*for(int i=1;i<=255;i++)
			System.out.println(i+" "+(char)i);*/
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Stemming? (y/n) : ");
		boolean stem = br.readLine().equals("y") ? true : false;
		System.out.println("Remove Stop words? (y/n) : ");
		boolean stop = br.readLine().equals("y") ? true : false;
		if(stem&&stop)
			docroot = "/Users/prachibhansali/Documents/IR/Assignment2/MultiFieldIndexing/Index-StemmerStopwords/";
		else if(stem&&!stop)
			docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StemmerOnly/";
		else if(!stem&&stop)
			docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StopWordOnly/";
		else docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-NoStemNoStop/";

		HashMap<String,String> headers = new HashMap<String,String>();	
		HashMap<String,String> documents = generateDocTextMapping(headers);
		printMapStringToFile("headers",headers);
		performIndexing(stem,stop,documents,headers);

	}

	private static void performIndexing(boolean stem, boolean stop, HashMap<String, String> documents, HashMap<String, String> headers) throws Exception {
		Iterator<String> docitr = documents.keySet().iterator();
		HashMap<String,Integer> terms = new HashMap<String,Integer>();
		HashMap<String,Integer> hterms = new HashMap<String,Integer>();
		HashMap<Integer,String> docs = new HashMap<Integer,String>();
		HashMap<Integer,String> hdocs = new HashMap<Integer,String>();
		HashMap<String,Integer> stopwords = stop? getStopWords() : new HashMap<String,Integer>();
		boolean firstentry=true;
		int countscans=0;
		int cf=0,hcf=0;
		int id = 1,doc_id=1;
		long offst=0,hoffst=0;
		while(docitr.hasNext())
		{
			HashMap<Integer,ArrayList<TermDocInfo>> termDocMapping = new HashMap<Integer,ArrayList<TermDocInfo>>();
			HashMap<Integer,ArrayList<TermDocInfo>> htermDocMapping = new HashMap<Integer,ArrayList<TermDocInfo>>();
			Catalog c =new Catalog();
			Catalog hc =new Catalog();

			countscans++;
			int counttemp=1;
			System.out.println(countscans);
			//while(avail_memory > docname.getBytes().length+buffer)
			while(counttemp<=1000&&docitr.hasNext())
			{
				String docname = docitr.next();
				int docid= doc_id++;
				docs.put(docid, docname);

				//System.out.println("Docname = "+docname);
				counttemp++;
				HashMap<String,ArrayList<Integer>> tokens = getTokens(stem,documents.get(docname),stopwords);

				HashMap<String,ArrayList<Integer>> htokens = headers.containsKey(docname) ? getTokens(stem,headers.get(docname),stopwords) :null;
				if(htokens!=null) hdocs.put(docid, docname);
				Iterator<String> tokenitr = tokens.keySet().iterator();
				while(tokenitr.hasNext())
				{
					String token = (String) tokenitr.next();
					int tokenID=0;
					if(!terms.containsKey(token) && !hterms.containsKey(token)) {
						tokenID=id++;
						terms.put(token,tokenID);
					}
					else tokenID=terms.containsKey(token) ? terms.get(token) : hterms.get(token);

					if(!terms.containsKey(tokenID)) terms.put(token,tokenID);
					//System.out.print("token = "+token);
					//fetch positions of token
					Iterator<Integer> positr = tokens.get(token).iterator();

					ArrayList<TermDocInfo> termdocarr = 
							termDocMapping.containsKey(tokenID)
							? termDocMapping.get(tokenID) : new ArrayList<TermDocInfo>();

							TermDocInfo termdoc = new TermDocInfo(docname,docid);
							while(positr.hasNext())
							{
								int posn = (int) positr.next();
								termdoc.addPosition(posn);
								cf++;
							}
							termdocarr.add(termdoc);
							termDocMapping.put(tokenID, termdocarr);
				}

				//System.out.println(docname+"  "+documents.get(docname));
				if(htokens!=null)
				{Iterator<String> htokenitr = htokens.keySet().iterator();
				while(htokenitr.hasNext())
				{
					String token = (String) htokenitr.next();
					int tokenID=0;
					if(!hterms.containsKey(token) && !terms.containsKey(token)) {
						tokenID=id++;
						hterms.put(token,tokenID);
					}
					else tokenID=hterms.containsKey(token) ? hterms.get(token) : terms.get(token);

					if(!hterms.containsKey(tokenID)) hterms.put(token,tokenID);
					//System.out.print("token = "+token);
					//fetch positions of token
					Iterator<Integer> positr = htokens.get(token).iterator();

					ArrayList<TermDocInfo> termdocarr = 
							htermDocMapping.containsKey(tokenID)
							? htermDocMapping.get(tokenID) : new ArrayList<TermDocInfo>();

							TermDocInfo termdoc = new TermDocInfo(docname,docid);
							while(positr.hasNext())
							{
								int posn = (int) positr.next();
								termdoc.addPosition(posn);
								hcf++;
							}
							termdocarr.add(termdoc);
							htermDocMapping.put(tokenID, termdocarr);
							//System.out.println(token+" "+tokens.size());
				}
				}
			}


			printToFile(termDocMapping,docroot+"doc-1.txt",c);
			printToFile(htermDocMapping,docroot+"header-1.txt",hc);
			c.printToFile(docroot+"catalog-1.txt");
			hc.printToFile(docroot+"hcatalog-1.txt");
			if(firstentry) {
				(new File(docroot+"doc-1.txt")).renameTo(new File(docroot+"index.txt"));
				(new File(docroot+"header-1.txt")).renameTo(new File(docroot+"hindex.txt"));
				c.printToFile(docroot+"catalog.txt");
				hc.printToFile(docroot+"hcatalog.txt");
				firstentry=false;
				continue;
			}
			else 
				if(countscans==85) {
					offst=MergeIndexWithDoc("doc-1.txt",docroot+"catalog.txt",docroot+"index.txt",c,true);
					hoffst=MergeIndexWithDoc("header-1.txt",docroot+"hcatalog.txt",docroot+"hindex.txt",hc,true);
				}
				else {
					MergeIndexWithDoc("doc-1.txt",docroot+"catalog.txt",docroot+"index.txt",c,false);
					MergeIndexWithDoc("header-1.txt",docroot+"hcatalog.txt",docroot+"hindex.txt",hc,false);
				}
		}
		printMapToFile(docroot+"Term-ID-Mappings",terms);
		printMapToFile(docs,docroot+"Doc-ID-Mappings");
		printMapToFile(docroot+"Header-Term-ID-Mappings",hterms);
		printMapToFile(hdocs,docroot+"Header-Doc-ID-Mappings");


		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(docroot+"index.txt",true)));
		String metadata = "\\"+terms.size()+"\\"+cf;
		pw.println(metadata);
		Catalog c = readCatalogIntoMemory(docroot+"catalog.txt");
		c.add(0, offst, metadata.getBytes().length);
		c.printToFile(docroot+"catalog.txt");
		pw.close();

		pw = new PrintWriter(new BufferedWriter(new FileWriter(docroot+"hindex.txt",true)));
		String headermetadata = "\\"+hterms.size()+"\\"+hcf;
		pw.println(headermetadata);
		Catalog hc = readCatalogIntoMemory(docroot+"hcatalog.txt");
		hc.add(0, hoffst, headermetadata.getBytes().length);
		hc.printToFile(docroot+"hcatalog.txt");
		pw.close();
	}


	private static void printMapToFile(String file,
			HashMap<String, Integer> map) throws FileNotFoundException {
		Iterator<String> itr = map.keySet().iterator();
		PrintWriter pw = new PrintWriter(file);
		while(itr.hasNext())
		{
			String key = itr.next();
			pw.println(key+" "+map.get(key));
		}
		pw.close();
	}

	private static void printMapStringToFile(String file,
			HashMap<String, String> map) throws FileNotFoundException {
		Iterator<String> itr = map.keySet().iterator();
		PrintWriter pw = new PrintWriter(file);
		while(itr.hasNext())
		{
			String key = itr.next();
			pw.println(key+" "+map.get(key));
		}
		pw.close();
	}


	private static void printMapToFile(HashMap<Integer, String> map,String file) throws FileNotFoundException {
		Iterator<Integer> itr = map.keySet().iterator();
		PrintWriter pw = new PrintWriter(file);
		while(itr.hasNext())
		{
			int key = (int) itr.next();
			pw.println(key+" "+map.get(key).toString());
		}
		pw.close();
	}


	private static long MergeIndexWithDoc(String file,String cfile,String indexfile,Catalog c,boolean b) throws Exception {
		Catalog catalog = readCatalogIntoMemory(cfile);
		RandomAccessFile index = new RandomAccessFile(indexfile,"rw");
		RandomAccessFile doc = new RandomAccessFile(docroot+file,"r");
		PrintWriter pw = new PrintWriter(docroot+"temp.txt");
		Catalog newc = new Catalog();
		long offset=0;

		Iterator<Integer> keys = catalog.getTermKeySet().iterator();
		while(keys.hasNext())
		{
			int termID = (int) keys.next();
			//System.out.println(termID+" "+catalog.get(termID).getSize());
			String indexedTerm = getIndexedTermInfo(catalog,index,termID);
			//System.out.println("Index term : "+termID+"****"+indexedTerm);

			if(!c.contains(termID))
			{
				String encodedIndexedTerm = indexedTerm;
				pw.println(encodedIndexedTerm);
				newc.add(termID, offset, encodedIndexedTerm.getBytes().length);
				offset+=encodedIndexedTerm.getBytes().length+1;
			}
			else {
				byte docb[] = getIndexedTermInfo(c,doc,termID).getBytes();
				//System.out.println("2");
				String news = MergeCatalogTerms(termID,indexedTerm.getBytes(),docb);
				pw.println(news);
				//System.out.println("4");
				newc.add(termID, offset, news.getBytes().length);
				offset+=news.getBytes().length+1;
			}
		}

		Iterator<Integer> itr = c.getTermKeySet().iterator();
		//System.out.println("6");
		while(itr.hasNext())
		{
			int termid = (int) itr.next();
			//System.out.println("present in the newly created catalog : "+termid);
			if(!newc.contains(termid))
			{
				String indexedTerm = getIndexedTermInfo(c,doc,termid);
				pw.println(indexedTerm);
				newc.add(termid, offset, indexedTerm.getBytes().length);
				offset+=indexedTerm.getBytes().length+1;
			}
		}
		//System.out.println("7");

		pw.close();
		newc.printToFile(cfile);

		new File(docroot+"temp.txt").renameTo(new File(indexfile));
		//System.out.println("next key please");
		if(b) return offset;
		else return -1;
	}

	private static String getIndexedTermInfo(Catalog c,RandomAccessFile doc,int termID) throws IOException
	{
		byte b[] = new byte[(int) c.get(termID).getSize()];
		doc.seek((int)c.get(termID).getOffset());
		doc.read(b);
		//System.out.println("indexed "+termID+" "+new String(b)+" "+b.length);
		return new String(b);
	}

	private static String MergeCatalogTerms(int termID, byte[] b, byte[] docb) {
		String mergedterm=""+termID;
		mergedterm+=mergeDocumentsForTerm(new String(b),new String(docb));
		return mergedterm.trim();
	}

	private static String mergeDocumentsForTerm(String str1,String str2)
	{
		//System.out.println(str1);
		String mergedterm=""+str1.substring(str1.indexOf('$'));
		HashMap<Integer,String> docids = new HashMap<Integer,String>();
		//Pattern p = Pattern.compile("(\\$[^\\$ ]+)");
		//Pattern docidp = Pattern.compile("(\\$([^\\*]+))");
		Pattern p = Pattern.compile("(\\$([^\\*]+))");
		Matcher m1 = p.matcher(str2);
		Matcher m2 = p.matcher(str1);
		String head= "\\$";
		String body="[^\\$ ]+";
		HashMap<Integer,String> dups = new HashMap<Integer,String>();
		while(m2.find())
		{
			//String docinfo = m2.group(0);
			int docid = Integer.parseInt(m2.group(2));

			//int docid = Integer.parseInt(docinfo.substring(docinfo.indexOf('$')+1, docinfo.indexOf('*')));
			docids.put(docid, "");
		}
		while(m1.find()){
			int docid = Integer.parseInt(m1.group(2));
			if(!docids.containsKey(docid));
			else dups.put(docid, "");
		}
		if(dups.size()==0) mergedterm+=str2.substring(str2.indexOf('$'));
		else {
			System.out.println("Very rare !!!!!!!!!!!!!!!!!!!!!!!!!!!");
			mergedterm=mergedterm.replaceAll(head+body, "");
			//mergedterm+=MergedDocInfo(docid,docinfo,docids.get(docid));
		}
		return mergedterm;
	}

	private static String MergedDocInfo(int docid,String docinfo1, String docinfo2) {
		String str="$"+docid;
		//System.out.println(docinfo1);
		Pattern p = Pattern.compile("(\\*([^\\*|$|\\$]+))");
		Matcher intm = p.matcher(docinfo1);
		Set<Integer> s = new HashSet<Integer>();
		while(intm.find())
		{
			//System.out.println(intm.group(2));
			s.add(Integer.parseInt(intm.group(2)));
		}
		intm = p.matcher(docinfo2);
		while(intm.find())
		{
			//System.out.println(intm.group(2));
			s.add(Integer.parseInt(intm.group(2)));
		}

		Iterator<Integer> pos = s.iterator();
		while(pos.hasNext())
			str+="*"+(int)pos.next();
		return str;
	}

	private static Catalog readCatalogIntoMemory(String file) throws Exception {
		Catalog c=new Catalog();
		c.loadFromFile(file);
		return c;
	}


	private static void printToFile(HashMap<Integer, ArrayList<TermDocInfo>> termDocMapping,
			String file,Catalog c) throws FileNotFoundException {
		Iterator<Integer> termsitr = termDocMapping.keySet().iterator();
		PrintWriter pw = new PrintWriter(file);
		long offset=0;
		while(termsitr.hasNext())
		{
			String line="";
			int term = (int)termsitr.next();
			//System.out.println(term.getName() + " " + term.getID()+" ");
			//line+=term+" "+termDocMapping.get(term).size()+" ";
			line+=""+term;
			for(int i=0;i<termDocMapping.get(term).size();i++)
			{
				String termdoc = termDocMapping.get(term).get(i).toString();
				line+=termdoc;
			}
			line = line.trim();
			//System.out.println("Printed to index : "+line.getBytes().length);
			pw.println(line);
			long size = line.getBytes().length;
			c.add(term, offset, size);
			offset+=size+1;
		}
		pw.close();	
	}

	private static HashMap<String, String> generateDocTextMapping(HashMap<String,String> headers) throws IOException {
		System.out.println("Tokenizing");
		File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/ap89_collection/");
		//File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/temp/");
		File[] files = folder.listFiles();
		HashMap<String,String> documents = new HashMap<String,String>();

		// Extract all documents in the files
		for(int i=0;i<files.length;i++)
			documents.putAll(extractDocs(files[i],headers));
		//System.out.println("*********"+documents.get("AP890101-0060"));
		return documents;
	}

	public static HashMap<String,ArrayList<Integer>> getTokens(boolean stem,String text, HashMap<String, Integer> stopwords) throws IOException
	{
		HashMap<String,ArrayList<Integer>> tokens = new HashMap<String,ArrayList<Integer>>();
		Pattern p = Pattern.compile("(\\w+)");
		Matcher m = p.matcher(text);
		int position=1;
		while(m.find()){
			String tok = m.group(1).trim().toLowerCase();
			if(!stopwords.containsKey(tok))
			{
				tok = stem? applyStemmer(tok) : tok;
				ArrayList<Integer> positions = tokens.containsKey(tok) ? tokens.get(tok) : new ArrayList<Integer>();
				positions.add(position++);
				tokens.put(tok,positions);
			}
		}
		return tokens;
	}

	private static String applyStemmer(String str) {
		Stemmer s = new Stemmer();
		char w[] = str.toCharArray();
		s.add(w, w.length);
		s.stem();
		return s.toString();
	}

	public static HashMap<String,String> extractDocs(File file,HashMap<String,String> headers) {
		BufferedReader br = null;
		System.out.println(file);
		HashMap<String,String> lst = new HashMap<String,String>();	
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			System.out.println("File " + file + " not found.");
		}
		try {
			String line = br.readLine();
			while(line!=null)
			{
				StringBuffer docnumber = new StringBuffer("");
				StringBuffer text = new StringBuffer("");
				StringBuffer htext = new StringBuffer("");

				while(line!=null && !line.contains("<DOC>"))
					line=br.readLine();
				if(line == null) break;

				// Extracting Doc Number as it is metadata
				while(!line.contains("<DOCNO>")) line=br.readLine();
				docnumber.append(line);
				if(!line.contains("</DOCNO>")) {
					while(!line.contains("</DOCNO>"))
					{
						docnumber.append(line);
						line = br.readLine();
					}
					docnumber.append(line);
				}

				// Filter Doc Number
				String docno = docnumber.toString();
				int endtag = docno.indexOf("</DOCNO>");
				docno = docno.substring(docno.indexOf("<DOCNO>") + 7, endtag).trim();
				line = line.substring(endtag + 8);

				do
				{
					int textendtag;
					while(!line.contains("<HEAD>") && !line.contains("</DOC>") && !line.contains("<TEXT>")) 
						line=br.readLine();
					if(line.contains("<HEAD>")) {
						line=line.substring(line.indexOf("<HEAD>")+6);
						while(!line.contains("</HEAD>"))
						{
							htext.append(" "+line);
							line=br.readLine();
						}
						textendtag = line.indexOf("</HEAD>");
						htext.append(" "+line.substring(0,textendtag));
					}
					else break;
					line=line.substring(textendtag + 7);
				}
				while(!line.contains("</DOC>") && !line.contains("<TEXT>"));
				//System.out.println("**"+line);
				
				String houtput = htext.toString().toLowerCase();
				//output = output.replaceAll("[^a-zA-z0-9.-]+", " ");
				String hformatoutput1 = houtput.replaceAll("_"," ");
				String hformatoutput = hformatoutput1.replaceAll("[^\\w\\d- ]+","");
				if(!hformatoutput.equals("")) headers.put(docno,hformatoutput);
				
				// Extracting content
				do
				{
					int textendtag;
					while(!line.contains("<TEXT>") && !line.contains("</DOC>")) {
						line=br.readLine();
						//System.out.println("***"+line);
						
					}
					if(line.contains("<TEXT>")) {
						line=line.substring(line.indexOf("<TEXT>")+6);
						while(!line.contains("</TEXT>"))
						{
							text.append(" "+line);
							line=br.readLine();
						}
						textendtag = line.indexOf("</TEXT>");
						text.append(" "+line.substring(0,textendtag));
					}
					else break;
					line=line.substring(textendtag + 7);
				}
				while(!line.contains("</DOC>"));
				String output = text.toString().toLowerCase();
				//output = output.replaceAll("[^a-zA-z0-9.-]+", " ");
				String formatoutput1 = output.replaceAll("_"," ");
				String formatoutput = formatoutput1.replaceAll("[^\\w\\d- ]+","");
				lst.put(docno,formatoutput);
				line = line.substring(line.indexOf("</DOC>") + 6);
			}
			br.close();
		}
		catch (IOException e) {
			System.out.println("File " + file + " not readable.");
		}
		return lst;
	}

	private static HashMap<String,Integer> getStopWords() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File(docroot+"stoplist.txt")));
		String stopword="";
		HashMap<String,Integer> stopwords = new HashMap<String,Integer>();
		while((stopword=br.readLine())!=null)
			stopwords.put(stopword.toLowerCase().trim(), 0);
		br.close();
		return stopwords;
	}


	private static String elongateBinaryString(String binaryString, int max) {
		while(binaryString.length() < max)
			binaryString=binaryString+"0";
		return binaryString;
	}

	private static String modifyBinaryString(String binaryString,int max) {
		if(binaryString.length() < max){
			while(binaryString.length() < max)
				binaryString="0"+binaryString;
		}
		else if(binaryString.length() > max) binaryString.substring(binaryString.length()-max, binaryString.length());
		return binaryString;
	}
}
