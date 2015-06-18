package com.ir.indexing.tokenizer;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileCompression {
	private static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Indexing/IndexStemmerStopwords/";
	static HashMap<Integer,String> wordEncodings = null;
	static HashMap<String,Integer> wordDecodings = null;
	static HashMap<String,String> encodings = null;
	static HashMap<String,String> decodings = null;

	public FileCompression() throws FileNotFoundException
	{
		wordEncodings = new HashMap<Integer,String>();
		wordDecodings = new HashMap<String,Integer>();
		encodings = new HashMap<String,String>();
		decodings = new HashMap<String,String>();
		createEncodings(wordEncodings,wordDecodings);
	}
	/*public static void main(String args[]) throws Exception
	{
		FileCompression fc = new FileCompression();
		String str = fc.performEncoding("prachibhansali", wordEncodings);
		System.out.println(str+" "+str.length());
		System.out.println(fc.performDecoding(str, wordDecodings) + " "+fc.performDecoding(str, wordDecodings).getBytes().length);
		//fc.decompress("");
	}*/

	public void compressIndex(String indexlocation) throws Exception
	{
		docroot = indexlocation;
		Catalog catalog = readCatalogIntoMemory();
		Catalog newc = new Catalog();
		RandomAccessFile index = new RandomAccessFile(docroot+"index.txt","rw");
		PrintWriter pw = new PrintWriter("encodedIndex");
		int len = (int) catalog.size();
		Iterator<Integer> keys = catalog.getTermKeySet().iterator();
		long offset = 0;
		while(keys.hasNext())
		{
			System.out.println(len--);
			int termID = (int) keys.next();
			String indexedTerm = getIndexedTermInfo(catalog,index,termID);
			String encodedStr = performEncoding(indexedTerm,wordEncodings);
			newc.add(termID, offset, encodedStr.getBytes().length);
			offset+=encodedStr.getBytes().length+1;
			pw.println(encodedStr);
			System.out.println("encoded");
		}
		pw.close();
		(new File("encodedIndex")).renameTo(new File(docroot+"index.txt"));
		newc.printToFile(docroot+"catalog.txt");		
	}

	private static Catalog readCatalogIntoMemory() throws Exception {
		Catalog c=new Catalog();
		c.loadFromFile(docroot+"catalog.txt");
		return c;
	}	

	private static String getIndexedTermInfo(Catalog c,RandomAccessFile doc,int termID) throws IOException
	{
		byte b[] = new byte[(int) c.get(termID).getSize()];
		doc.seek((int)c.get(termID).getOffset());
		doc.read(b);
		return new String(b);
	}

	private String performEncoding(String str,HashMap<Integer,String> wordEncodings)
	{
		String encodedString="";
		StringBuffer encstr=new StringBuffer("");
		for(int i=0;i<str.length();i++)
			encstr.append(wordEncodings.get((int)(str.charAt(i))));
		System.out.println(encstr);
		Pattern p = Pattern.compile("([01]{42})");
		Matcher m = p.matcher(encstr);
		while(m.find())
		{
			String res = m.group(0);
			if(encodings.containsKey(res)) 
			{
				encodedString += encodings.get(res);
			}
			else {
				encodedString+=encodeThreeBytes(encodings,res);
			}
		}
		int remchars = encstr.length()%42;
		if(remchars > 0)
		{
			String s = elongateBinaryString(encstr.substring(encstr.length()-remchars),42);
			encodedString += encodeThreeBytes(encodings,s);
		}
		return encodedString;
	}

	public static String encodeThreeBytes(HashMap<String, String> encodings,String encstr) {
		Pattern p = Pattern.compile("([01]{7})");
		Matcher m = p.matcher(encstr);
		String encodedstr = "";
		while(m.find())
		{
			String bstr = m.group(0);
			encodedstr+=Character.toChars(Integer.parseInt(bstr,2))[0];
		}
		encodings.put(encstr, encodedstr);
		return encodedstr;
	}



	private static String elongateBinaryString(String binaryString, int max) {
		while(binaryString.length() < max)
			binaryString=binaryString+"0";
		return binaryString;
	}

	private static void createEncodings(HashMap<Integer, String> wordEncodings, HashMap<String, Integer> wordDecodings2) throws FileNotFoundException {
		int diff = 123-92;
		for(int i=92,j=32;i<=122&&j<=62;i++,j++)
		{
			String res = modifyBinaryString(Integer.toBinaryString(diff--),5);
			wordEncodings.put(i,"1"+res);
			wordEncodings.put(j,"0"+res);
			wordDecodings2.put("1"+res,i);
			wordDecodings2.put("0"+res,j);
		}
	}

	private static String modifyBinaryString(String binaryString,int max) {
		if(binaryString.length() < max){
			while(binaryString.length() < max)
				binaryString="0"+binaryString;
		}
		else if(binaryString.length() > max) binaryString.substring(binaryString.length()-max, binaryString.length());
		return binaryString;
	}

	private String performDecoding(String encodedstr,HashMap<String, Integer> wordDecodings)
	{
		String decodedString="";
		String binaryString="";
		for(int i=0;i<encodedstr.length();i++)
		{
			int c = (int)encodedstr.charAt(i);
			binaryString+=modifyBinaryString(Integer.toBinaryString(c),7);
		}
		System.out.println(binaryString);
		
		/*int i=0;
		while((i+5)<=binaryString.length())
		{
			String s = binaryString.substring(i, i+6);
			if(!s.equals("000000")) decodedString+=Character.toChars(wordDecodings.get(s))[0];
			//System.out.println(s);
			i+=6;
		}*/
		Pattern p = Pattern.compile("([01]{42})");
		Matcher m = p.matcher(binaryString);
		while(m.find())
		{
			String res = m.group(0);
			if(decodings.containsKey(res)) 
			{
				decodedString += decodings.get(res);
			}
			else {
				decodedString+=decodeFourBytes(res);
			}
		}
		return decodedString;
	}

	private String decodeFourBytes(String res) {
		Pattern p = Pattern.compile("([01]{6})");
		Matcher m = p.matcher(res);
		String decodedstr = "";
		while(m.find())
		{
			String bstr = m.group(0);
			if(!bstr.equals("000000")) decodedstr+=Character.toChars(wordDecodings.get(bstr))[0];
		}
		decodings.put(res, decodedstr);
		return decodedstr;
	}

	public void decompress(String indexlocation) throws Exception {
		docroot = indexlocation;
		Catalog catalog = readCatalogIntoMemory();
		Catalog newc = new Catalog();
		RandomAccessFile index = new RandomAccessFile(docroot+"index.txt","rw");
		PrintWriter pw = new PrintWriter("decodedIndex");
		int len = (int) catalog.size();
		Iterator<Integer> keys = catalog.getTermKeySet().iterator();
		long offset = 0;
		while(keys.hasNext())
		{
			System.out.println(len--);
			int termID = (int) keys.next();
			String indexedTerm = getIndexedTermInfo(catalog,index,termID);
			String decodedStr = performDecoding(indexedTerm,wordDecodings);
			newc.add(termID, offset, decodedStr.getBytes().length);
			offset+=decodedStr.getBytes().length+1;
			pw.println(decodedStr);
			System.out.println("decoded");
		}
		pw.close();
		(new File("decodedIndex")).renameTo(new File(docroot+"index.txt"));
		newc.printToFile(docroot+"catalog.txt");
	}
}
