package com.ir.indexing.tokenizer;


import java.io.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {

	public static void main(String[] args) throws IOException {
		System.out.println("Tokenizing"+("".equals(null)));
		File folder = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/ap89_collection/");
		File[] files = folder.listFiles();
		HashMap<String,String> documents = new HashMap<String,String>();
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("index.txt",true)));
		// Extract all documents in the files
		for(int i=0;i<files.length;i++)
			documents.putAll(extractDocs(files[i]));
			
		// For every document extract tokens starting wit
		Iterator<String> docitr = documents.keySet().iterator();
		
		while(docitr.hasNext())
		{
			String docname = (String) docitr.next();
			HashMap<String,ArrayList<Integer>> tokens = getTokens(documents.get(docname),"A");
			Iterator tokenitr = tokens.keySet().iterator();
			String pos="";
			while(tokenitr.hasNext())
			{
				String token = (String) tokenitr.next();
				Iterator positr = tokens.get(token).iterator();
				while(positr.hasNext())
					pos+=positr.next()+" ";
				String str = token+" "+token.hashCode()+" 1 "+docname+" "+pos.length()+" "+pos.trim()+" ";
				pw.println(str);
			}
		}
		pw.close();
	}

	public static HashMap<String,ArrayList<Integer>> getTokens(String text,String alphabet)
	{
		HashMap<String,ArrayList<Integer>> tokens = new HashMap<String,ArrayList<Integer>>();
		Pattern p = Pattern.compile("(["+alphabet.toUpperCase()+alphabet.toLowerCase()+"]+\\w+(\\.?\\w+)*)");
		Matcher m = p.matcher(text);
		System.out.println("Get Tokens");
		while(m.find()){
			System.out.println(m.group(1));
			System.out.println(m.start(1)+" "+m.end(1));
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
