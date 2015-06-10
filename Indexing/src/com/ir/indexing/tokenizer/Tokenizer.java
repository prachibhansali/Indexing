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

public class Tokenizer {

	static Catalog catalog;
	static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/";
	public static void main(String[] args) throws Exception {
		catalog = new Catalog();
		HashMap<String,String> documents = generateDocTextMapping();
		//System.out.println(new String("here is my name".getBytes());
		// For every document extract tokens starting wit
		performIndexing(documents);
		//System.out.println(mergeDocumentsForTerm("-1360493730*558$2100064923*1271$-2112582739*1813*1686*103*1513*1467$-383190895*1682","$2100064923*1669$-522519784*2542$-522518760*783$-205728136*1656$-2120314277*556$586286900*440$-2120313255*1168$1287149425*699$-205728139*1203$-205728143*421$-337577418*1115$1287149401*1188$586286870*3395$-205728168*791$-2112583693*741$1347976625*17$1347976626*802*946*1755*2396*159$-2120315275*743$-205728173*1308$2100063966*1523*1623*1735$-337577446*668"+"958937384$-2051758571*803$-522520808*840$-522520647*883$1347975483*1823$460471798*2463$-2051757613*1091$2054448476*2630$460470906*483$-2120315273*547*91$-1225081068*3274$468202264*2687$468203226*1935$-1299670436*1027$2107794138*1721$-383192847*1572$-522519776*1285$1302359004*980") );
	}


	private static void performIndexing(HashMap<String, String> documents) throws Exception {
		Iterator<String> docitr = documents.keySet().iterator();
		HashMap<String,Integer> terms = new HashMap<String,Integer>();
		HashMap<Integer,String> docs = new HashMap<Integer,String>();

		boolean firstentry=true;
		int countscans=0;
		int id = 10001;
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
				docs.put(docname.hashCode(), docname);
				//System.out.println("Docname = "+docname);
				counttemp++;
				HashMap<String,ArrayList<Integer>> tokens = getTokens(documents.get(docname));
				if(docname.equals("AP890103-0201")){
				Iterator i = tokens.get("20s").iterator();
				while(i.hasNext())
				System.out.println("here"+i.next());
				}
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

							TermDocInfo termdoc = new TermDocInfo(docname);
							while(positr.hasNext())
								{
								int posn = (int) (int)positr.next();
								termdoc.addPosition(posn);
								}
							termdocarr.add(termdoc);
							termDocMapping.put(tokenID, termdocarr);
							//System.out.println(token+" "+tokens.size());
				}
			}
			printToFile(termDocMapping,docroot+"doc-1.txt",c);
			c.printToFile(docroot+"catalog-1.txt");
			if(firstentry) {
				//System.out.println("Merging with index first time");
				//printToFile(termDocMapping,docroot+"index.txt",c);
				(new File(docroot+"doc-1.txt")).renameTo(new File(docroot+"index.txt"));
				c.printToFile(docroot+"catalog.txt");
				firstentry=false;
				continue;
			}
			else {
				//System.out.println("Merging with index ============ " + countscans);
				MergeIndexWithDoc("doc-1.txt",c);
			}
		}
		printMapToFile(docroot+"Term-ID-Mappings",terms);
		printMapToFile(docs,docroot+"Doc-ID-Mappings");
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
				String news = MergeCatalogTerms(termID,indexedTerm.getBytes(),docb);
				pw.println(news);
				newc.add(termID, offset, news.getBytes().length);
				offset+=news.getBytes().length+1;
			}
		}

		Iterator<Integer> itr = c.getTermKeySet().iterator();
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
		if(termID==49653)
		System.out.println("to be merged terms : "+termID+" "+new String(b)+"***"+new String(docb));
		//System.out.println(termID);
		mergedterm+=mergeDocumentsForTerm(termID,new String(b),new String(docb));
		/*String str1 = extractDocInfo(new String(b));
		String str2 = extractDocInfo(new String(docb));
		//System.out.println("to be merged terms : "+str1+" "+str2);
		HashMap<Integer, ArrayList<Integer>> ti2 = new HashMap<Integer, ArrayList<Integer>>();
		getTermInfos(str1,ti2);
		getTermInfos(str2,ti2);
		 */
		//String str1 = extractDocInfo(new String(b));
		//String str2 = extractDocInfo(new String(docb));
		//HashMap<Integer,Set<Integer>> docids = new HashMap<Integer,Set<Integer>>();
		//mergeDocumentsForTerm(new String(b),docids);
		//mergeDocumentsForTerm(new String(docb),docids);
/*..-522520679
		Iterator<Integer> keys = docids.keySet().iterator();
		//mergedterm+=ti2.size()+" ";
		while(keys.hasNext())
		{
			int key = (int) keys.next();
			//System.out.println("keys " + key);
			mergedterm+="$"+key;
			Iterator<Integer> pitr=docids.get(key).iterator();
			while(pitr.hasNext())
				mergedterm+="*"+(int)pitr.next();
		}
*/
		return mergedterm.trim();
	}

	private static Set<Integer> extractPosns(String[] split) {
		Set<Integer> set = new HashSet<Integer>();
		for(int i=1;i<split.length;i++)
			set.add(Integer.parseInt(split[i]));
		return set;	
	}


	private static String extractDocInfo(String str1, int s_index) {
		int e_index = str1.indexOf('$', s_index+1);
		if(e_index==-1) e_index = str1.length();
		String docinfo = str1.substring(s_index, e_index);
		return docinfo;
	}

	private static int extractDocID(String str1){
		return Integer.parseInt(str1.substring(str1.indexOf('$')+1, str1.indexOf("*")));
	}

	private static String mergeDocumentsForTerm(int termid,String str1,String str2)
	{
		String mergedterm="";
		HashMap<Integer,String> docids = new HashMap<Integer,String>();
		Pattern p = Pattern.compile("(\\$[^\\$ ]+)");
		Pattern docidp = Pattern.compile("(\\$([^\\*]+))");
		Matcher m1 = p.matcher(str1+" "+str2);
		String head= "\\$";
		String body="[^\\$ ]+";
		while(m1.find()){
			String docinfo = m1.group(0);
			Matcher docidm = docidp.matcher(docinfo);
			int docid=0;
			while(docidm.find())
			{
				docid=Integer.parseInt(docidm.group(2));
				break;
			}
			if(!docids.containsKey(docid)) {
				mergedterm+=docinfo;
				docids.put(docid, docinfo);
			}
			else {
				System.out.println(termid+" "+docid);
				System.out.println("Very rare !!!!!!!!!!!!!!!!!!!!!!!!!!!");
				//System.out.println(head+docid+body);
				mergedterm=mergedterm.replaceAll(head+docid+body, "");
				mergedterm+=MergedDocInfo(docid,docinfo,docids.get(docid));				
			}
		}
		return mergedterm;
		/*
		while(str1!=""&&str1.indexOf('$')!=-1)
		{
			String docinfo = extractDocInfo(str1,str1.indexOf('$'));
			int docid = extractDocID(docinfo);
			Set<Integer> positions = extractPosns(docinfo.split("\\*"));
			if(docids.containsKey(docid)) MergeTerms(docids,positions,docid);
			else docids.put(docid, positions);
			str1=str1.substring(docinfo.length());
		}*/
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


	private static void MergeTerms(HashMap<Integer, Set<Integer>> docids,
			Set<Integer> positions, int docid) {
		Set<Integer> docs = docids.get(docid);
		docs.addAll(positions);
		docids.put(docid, docs);
	}


	private static String extractDocInfo(String str)
	{
		String str1="";
		String strarr1[] = str.split(" ");
		for(int i=1;i<strarr1.length;i++)
			str1+=strarr1[i]+" ";
		return str1;
	}

	private static void getTermInfos(String str1, HashMap<Integer, ArrayList<Integer>> hashMap) {
		String[] s = str1.split(" ");
		int index=1;
		for(int i=0;i<Integer.parseInt(s[0]);i++)
		{
			int docID = Integer.parseInt(s[index]);
			int j=0;
			ArrayList<Integer> posns = new ArrayList<Integer>();
			for(j=0;j<Integer.parseInt(s[index+1]);j++)
			{
				//System.out.println("..."+s[index+2+j]+"...");
				posns.add(Integer.parseInt(s[index+2+j]));
			}

			index=index+2+j;
			if(hashMap.containsKey(docID)) 
				posns=mergePositions(posns,hashMap.get(docID));

			hashMap.put(docID,posns);
		}
	}

	private static ArrayList<Integer> mergePositions(ArrayList<Integer> posns,
			ArrayList<Integer> arrayList) {
		posns.addAll(arrayList);
		Set<Integer> set = new HashSet<Integer>(posns);
		return new ArrayList<Integer>(set);
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
	public static HashMap<String,ArrayList<Integer>> getTokens(String text) throws IOException
	{
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("terms",true)));
		//System.out.println(text);
		HashMap<String,ArrayList<Integer>> tokens = new HashMap<String,ArrayList<Integer>>();
		Pattern p = Pattern.compile("(\\w+(\\.?\\w+)*)");
		Matcher m = p.matcher(text);
		while(m.find()){
			
			pw.println(m.group(1));
			//System.out.println(m.start(1)+" "+m.end(1));
			String tok = m.group(1).trim().toLowerCase();
			//if(m.group(1).equalsIgnoreCase("20s"))  System.out.println(tokens.containsKey(tok));
			ArrayList<Integer> positions = tokens.containsKey(tok) ? tokens.get(tok) : new ArrayList<Integer>();
			//if(tok.equals("20s")) System.out.println(positions.size());
			positions.add(m.start(1));
			//if(m.group(1).equalsIgnoreCase("20s")) System.out.println("Adding..."+m.group(1)+".."+m.start(1));
			tokens.put(tok,positions);
			
		}
		pw.close();
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
				String output = text.toString();
				//output = output.replaceAll("[^a-zA-z0-9.-]+", " ");
				String formatoutput = output.replaceAll("[^\\w\\d- ]+","");

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


}
