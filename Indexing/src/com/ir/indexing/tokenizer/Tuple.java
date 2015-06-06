package com.ir.indexing.tokenizer;

public class Tuple {
	int termID;
	long offset;
	long size;
	
	public Tuple(int termid,long offset,long size)
	{
		setTermID(termid);
		setOffset(offset);
		setSize(size);
	}
	
	public int getTermID()
	{
		return termID;
	}
	
	public long getOffset()
	{
		return offset;
	}

	public long getSize()
	{
		return size;
	}
	
	public void setTermID(int termid)
	{
		termID=termid;
	}
	
	public void setOffset(long offset)
	{
		this.offset=offset;
	}
	
	public void setSize(long size)
	{
		this.size=size;
	}
	
	public void setTuple(int termid,long offset,long size)
	{
		setTermID(termid);
		setOffset(offset);
		setSize(size);
	}
	
	public Tuple getTuple()
	{
		return this;
	}
}
