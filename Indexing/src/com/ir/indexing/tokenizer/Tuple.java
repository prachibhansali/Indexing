package com.ir.indexing.tokenizer;

public class Tuple {
	//int termID;
	private long offset;
	private long size;
	
	public Tuple(long offset,long size)
	{
		//setTermID(termid);
		setOffset(offset);
		setSize(size);
	}
	
	/*public int getTermID()
	{
		return termID;
	}
	
	public void setTermID(int termid)
	{
		termID=termid;
	}
	*/
	
	public long getOffset()
	{
		return offset;
	}

	public long getSize()
	{
		return size;
	}
	
	public void setOffset(long offset)
	{
		this.offset=offset;
	}
	
	public void setSize(long size)
	{
		this.size=size;
	}
	
	public void setTuple(long offset,long size)
	{
		//setTermID(termid);
		setOffset(offset);
		setSize(size);
	}
	
	public Tuple getTuple()
	{
		return this;
	}
	
	public String toString()
	{
		return new String(offset+" "+size);
	}
}
