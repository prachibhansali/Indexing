package com.ir.indexing.tokenizer;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {

	static Catalog catalog;
	static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/";
	public static void main(String[] args) throws IOException {
		catalog = new Catalog();
		HashMap<String,String> documents = generateDocTextMapping();

		// For every document extract tokens starting wit
		performIndexing(documents);
	}


	private static void performIndexing(HashMap<String, String> documents) throws FileNotFoundException {
		Iterator<String> docitr = documents.keySet().iterator();
		long buffer = 50l; // buffer for memory
		int countscans=0;
		while(docitr.hasNext())
		{
			long avail_memory = Runtime.getRuntime().freeMemory();
			HashMap<Term,TermDocInfo> termDocMapping = new HashMap<Term,TermDocInfo>();
			String docname = docitr.next();
			System.out.println(avail_memory);
			while(avail_memory > docname.getBytes().length+buffer)
			{
				countscans++;
				HashMap<String,ArrayList<Integer>> tokens = getTokens(documents.get(docname));
				String pos="";
				Iterator tokenitr = tokens.keySet().iterator();
				while(tokenitr.hasNext())
				{
					String token = (String) tokenitr.next();

					//fetch positions of token
					Iterator positr = tokens.get(token).iterator();
					TermDocInfo termdoc = termDocMapping.containsKey(token)? termDocMapping.get(token) : new TermDocInfo(docname);
					while(positr.hasNext())
						termdoc.addPosition((int)positr.next());
					termDocMapping.put(new Term(token), termdoc);
					System.out.println(token+" "+tokens.size());
				}
				if(docitr.hasNext()) docname = docitr.next();
				else break;
			}
			printToFile(termDocMapping,docroot+"doc"+countscans+".txt");
			if(countscans==1) continue;
			else 
				MergeIndexWithDoc(docroot+"doc"+countscans+".txt");
		}
	}


	private static void MergeIndexWithDoc(String string) {
		
	}


	private static void printToFile(HashMap<Term, TermDocInfo> termDocMapping,
			String string) throws FileNotFoundException {
		Iterator<Term> termsitr = termDocMapping.keySet().iterator();
		PrintWriter pw = new PrintWriter("index.txt");
		while(termsitr.hasNext())
		{
			Term term = (Term)termsitr.next();
			System.out.println(term.getName() + " " + term.getID()+" ");
			pw.print(term.getName() + " " + term.getID()+" ");
			String termdoc = termDocMapping.get(term).toString();
			pw.print(termdoc);
			pw.println();
		}
		pw.close();		
	}


	private static HashMap<String, String> generateDocTextMapping() throws IOException {
		System.out.println("Tokenizing");
		File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/ap89_collection/temp/");
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
	public static HashMap<String,ArrayList<Integer>> getTokens(String text)
	{
		//System.out.println(text);
		HashMap<String,ArrayList<Integer>> tokens = new HashMap<String,ArrayList<Integer>>();
		Pattern p = Pattern.compile("(\\w+(\\.?\\w+)*)");
		Matcher m = p.matcher(text);
		while(m.find()){
			//System.out.println(m.group(1));
			//System.out.println(m.start(1)+" "+m.end(1));
			ArrayList<Integer> positions = tokens.containsKey(m.group(1)) ? tokens.get(m.group(1)) : new ArrayList<Integer>();
			positions.add(m.start());
			tokens.put(m.group(1),positions);
		}
		return tokens;
	}

	public static HashMap<String,String> extractDocs(File file) {
		BufferedReader br = null;
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
