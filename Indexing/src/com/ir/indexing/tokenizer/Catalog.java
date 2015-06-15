package com.ir.indexing.tokenizer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

public class Catalog {
	private HashMap<Integer,Tuple> catalog;
	
	public Catalog()
	{
		catalog = new HashMap<Integer,Tuple>();
	}
	
	public void add(int termID,long offset,long size)
	{
		catalog.put(termID, new Tuple(offset,size));
	}
	
	public Tuple getTupleAtIndex(int index)
	{
		return catalog.get(index);
	}
	
	public void printToFile(String filename) throws FileNotFoundException
	{
		PrintWriter pw = new PrintWriter(filename);
		Iterator<Integer> itr = getTermKeySet().iterator();
		while(itr.hasNext()){
			int termid = (int)itr.next();
			pw.println(termid+" "+catalog.get(termid).toString());
			
		}
		pw.close();
	}
	
	public Catalog loadFromFile(String filename) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = "";
		while((line=br.readLine())!=null)
		{
			String[] parts = line.split(" ");
			if(parts.length<=2) {
				br.close();
				throw new Exception("Incorrect format of file");
			}
			Tuple t = new Tuple(Long.parseLong(parts[1]),Long.parseLong(parts[2]));
			catalog.put(Integer.parseInt(parts[0]),t);
		}
		br.close();
		return this;
	}
	
	public long size(){
		return catalog.size();
	}
	
	public Set<Integer> getTermKeySet()
	{
		return catalog.keySet();
	}
	
	public boolean contains(int termID)
	{
		return catalog.containsKey(termID);
	}

	public Tuple get(int termID) {
		return catalog.get(termID);
	}

	public void deleteID(int id) {
		catalog.remove(id);
	}
}
