package com.ir.indexing.tokenizer;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ir.indexing.stemmer.*;

public class StopwordStemmerTokenizer {

	static Catalog catalog;
	static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StemmerStopwords/";
	public static void main(String[] args) throws Exception {
		catalog = new Catalog();
		HashMap<String,String> documents = generateDocTextMapping();
		performIndexing(documents);
		//System.out.println(mergeDocumentsForTerm("-1360493730*558$2100064923*1271$-2112582739*1813*1686*103*1513*1467$-383190895*1682","$2100064923*1669$-522519784*2542$-522518760*783$-205728136*1656$-2120314277*556$586286900*440$-2120313255*1168$1287149425*699$-205728139*1203$-205728143*421$-337577418*1115$1287149401*1188$586286870*3395$-205728168*791$-2112583693*741$1347976625*17$1347976626*802*946*1755*2396*159$-2120315275*743$-205728173*1308$2100063966*1523*1623*1735$-337577446*668"+"958937384$-2051758571*803$-522520808*840$-522520647*883$1347975483*1823$460471798*2463$-2051757613*1091$2054448476*2630$460470906*483$-2120315273*547*91$-1225081068*3274$468202264*2687$468203226*1935$-1299670436*1027$2107794138*1721$-383192847*1572$-522519776*1285$1302359004*980") );
	}


	private static void performIndexing(HashMap<String, String> documents) throws Exception {
		Iterator<String> docitr = documents.keySet().iterator();
		HashMap<String,Integer> terms = new HashMap<String,Integer>();
		HashMap<Integer,String> docs = new HashMap<Integer,String>();
		HashMap<String,Integer> stopwords = getStopWords();
		boolean firstentry=true;
		int countscans=0;
		int id = 1,doc_id=1;
		while(docitr.hasNext())
		{
			HashMap<Integer,ArrayList<TermDocInfo>> termDocMapping = new HashMap<Integer,ArrayList<TermDocInfo>>();
			Catalog c =new Catalog();
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
				HashMap<String,ArrayList<Integer>> tokens = getTokens(documents.get(docname),stopwords);

				Iterator<String> tokenitr = tokens.keySet().iterator();
				while(tokenitr.hasNext())
				{
					String token = (String) tokenitr.next();
					int tokenID=0;
					if(!terms.containsKey(token)) {
						tokenID=id++;
						terms.put(token,tokenID);
					}
					else tokenID=terms.get(token);

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
								int posn = (int) (int)positr.next();
								termdoc.addPosition(posn);
							}
							if(docid==245) 
								System.out.println("found "+token+" in"+ " "+docname);
							termdocarr.add(termdoc);
							termDocMapping.put(tokenID, termdocarr);
							//System.out.println(token+" "+tokens.size());
				}
			}
			
			if(termDocMapping.containsKey(230)) 
			{for(int i=0;i<termDocMapping.get(230).size();i++)
			if((termDocMapping.get(230).get(i).toString()).indexOf("$245*")!=-1)
				System.out.println("ALERT!!!!"+termDocMapping.get(230).get(i).toString());}
			else System.out.println("Not present yet");
			
			printToFile(termDocMapping,docroot+"doc-1.txt",c);
			c.printToFile(docroot+"catalog-1.txt");
			if(firstentry) {
				(new File(docroot+"doc-1.txt")).renameTo(new File(docroot+"index.txt"));
				c.printToFile(docroot+"catalog.txt");
				firstentry=false;
				continue;
			}
			else MergeIndexWithDoc("doc-1.txt",c);
		}
		printMapToFile(docroot+"Term-ID-Mappings",terms);
		printMapToFile(docs,docroot+"Doc-ID-Mappings");

		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(docroot+"index.txt",true)));
		pw.println("\\"+terms.size());

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


	private static void MergeIndexWithDoc(String file,Catalog c) throws Exception {
		Catalog catalog = readCatalogIntoMemory();
		RandomAccessFile index = new RandomAccessFile(docroot+"index.txt","rw");
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
				pw.println(indexedTerm);
				newc.add(termID, offset, indexedTerm.getBytes().length);
				offset+=indexedTerm.getBytes().length+1;
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
		newc.printToFile(docroot+"catalog.txt");

		new File(docroot+"temp.txt").renameTo(new File(docroot+"index.txt"));
		//System.out.println("next key please");

	}

	private static String getIndexedTermInfo(Catalog c,RandomAccessFile doc,int termID) throws IOException
	{
		byte b[] = new byte[(int) c.get(termID).getSize()];
		doc.seek((int)c.get(termID).getOffset());
		doc.read(b);
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
			//String docinfo = m1.group(0);
			//int docid = Integer.parseInt(docinfo.substring(docinfo.indexOf('$')+1, docinfo.indexOf('*')));
			int docid = Integer.parseInt(m1.group(2));
			//System.out.println(docid);
			if(!docids.containsKey(docid));
			else dups.put(docid, "");
			/*else {
				System.out.println("Very rare !!!!!!!!!!!!!!!!!!!!!!!!!!!");
				//System.out.println(head+docid+body);
				mergedterm=mergedterm.replaceAll(head+docid+body, "");
				mergedterm+=MergedDocInfo(docid,docinfo,docids.get(docid));				
			}*/
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

	private static Catalog readCatalogIntoMemory() throws Exception {
		Catalog c=new Catalog();
		c.loadFromFile(docroot+"catalog.txt");
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
			//if(term==110327427) System.out.println("Printed to index : "+line+" " +termDocMapping.get(term).size());
			pw.println(line);
			long size = line.getBytes().length;
			c.add(term, offset, size);
			offset+=size+1;
		}
		pw.close();	
	}

	private static String removeSpecialChars(String query) {
		query=query.substring(0,query.trim().length()-1);
		query=query.replaceAll(",", "")
				.replaceAll("\\(", "")
				.replaceAll("\\)", "")
				.replaceAll("\"", "")
				.replaceAll("\\.", "");
		return query;
	}
	private static HashMap<String, String> generateDocTextMapping() throws IOException {
		System.out.println("Tokenizing");
		File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/ap89_collection/");
		//File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/temp/");
		File[] files = folder.listFiles();
		HashMap<String,String> documents = new HashMap<String,String>();

		// Extract all documents in the files
		for(int i=0;i<files.length;i++)
			documents.putAll(extractDocs(files[i]));
		return documents;
	}


	/*				String token = (String) tokenitr.next();
	Iterator positr = tokens.get(token).iterator();
	while(positr.hasNext())
		pos+=positr.next()+" ";
	int termID = token.hashCode();
	String str = token+" "+termID+" 1 "+docname+" "+docname.hashCode()+" "+pos.length()+" "+pos.trim()+" ";
	pw.println(str);

	// ADD CODE TO INCLUDE TERM AND SPACE IN CATALOG
	Tuple prevTuple = catalog.getTupleAtIndex(0);
	catalog.add(termID, prevTuple.getOffset()+prevTuple.getSize(), str.getBytes("UTF-8").length);*/
	public static HashMap<String,ArrayList<Integer>> getTokens(String text, HashMap<String, Integer> stopwords) throws IOException
	{
		HashMap<String,ArrayList<Integer>> tokens = new HashMap<String,ArrayList<Integer>>();
		Pattern p = Pattern.compile("(\\w+)");
		Matcher m = p.matcher(text);
		int position=1;
		while(m.find()){
			String tok = m.group(1).trim().toLowerCase();
			if(!stopwords.containsKey(tok))
			{
				Stemmer s = new Stemmer();
				char[] c = tok.toCharArray();
				s.add(c,c.length);
				s.stem();
				//if(!tok.equals(s.toString())) System.out.println(tok+" " +s.toString());
				tok = s.toString();
				ArrayList<Integer> positions = tokens.containsKey(tok) ? tokens.get(tok) : new ArrayList<Integer>();
				positions.add(position++);
				tokens.put(tok,positions);
			}
		}
		return tokens;
	}

	public static HashMap<String,String> extractDocs(File file) {
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

				// Extracting content
				do
				{
					int textendtag;
					while(!line.contains("<TEXT>") && !line.contains("</DOC>")) line=br.readLine();
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
				if(formatoutput.indexOf("_")!=-1) System.out.println(docno);
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


}
