package com.ir.indexing.tokenizer;

import java.util.ArrayList;

public class Catalog {
	ArrayList<Tuple> catalog;
	
	public Catalog()
	{
		catalog = new ArrayList<Tuple>();
	}
	
	public void add(int termID,long offset,long size)
	{
		catalog.add(new Tuple(termID, offset, size));
	}
	
}
