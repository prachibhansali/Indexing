package com.ir.indexing.tokenizer;

import java.util.ArrayList;

public class TermDocInfo {
	String docname;
	int docID;
	ArrayList<Integer> arr;
	
	public TermDocInfo(String docname)
	{
		arr = new ArrayList<Integer>();
		this.docname=docname;
		this.docID=docname.hashCode();
	}
	
	public void addPosition(int posn)
	{
		arr.add(posn);
	}
	
	public ArrayList<Integer> getPositions()
	{
		return arr;
	}
	
	public int getTermFreq()
	{
		return arr.size();
	}
	
	public String toString()
	{
		String output=docname+" "+docID+" ";
		for(int i=0;i<arr.size();i++)
			output+=arr.get(i)+" ";
		return output.trim();
	}
	
}
