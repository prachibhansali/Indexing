package com.ir.indexing.tokenizer;

import java.util.LinkedList;

public class Catalog {
	LinkedList<Tuple> catalog;
	
	public Catalog()
	{
		catalog = new LinkedList<Tuple>();
	}
	
	public void add(int termID,long offset,long size)
	{
		catalog.addFirst(new Tuple(termID, offset, size));
	}
	
	public Tuple getTupleAtIndex(int index)
	{
		return catalog.get(index);
	}
	
}
