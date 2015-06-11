package com.ir.indexing.tokenizer;

import java.util.ArrayList;

public class TermDocInfo {
	String docname;
	int docID;
	ArrayList<Integer> arr;
	
	public TermDocInfo(String docname,int docID,ArrayList<Integer> a)
	{
		arr = a;
		this.docname = docname;
		this.docID = docID;
	}
	
	public TermDocInfo(String docname,int id)
	{
		arr = new ArrayList<Integer>();
		this.docname=docname;
		this.docID=id;
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
		//String output=docID+" "+arr.size()+" ";
		String output="$"+docID;
		for(int i=0;i<arr.size();i++)
			output+="*"+arr.get(i);
		//if(docID==110327427) System.out.println(output+ " "+arr.size());
		return output.trim();
	}
	
	public String nameIDMapping()
	{
		return (docID + " " +docname);
	}
	
}
