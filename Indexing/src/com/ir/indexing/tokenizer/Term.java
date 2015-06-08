package com.ir.indexing.tokenizer;

public class Term {
	String name;
	int ID;
	
	public Term(String term)
	{
		this.name = term;
		ID = term.hashCode();
	}
	
	public int getID()
	{
		return ID;
	}
	
	public String getName()
	{
		return name;
	}
}
