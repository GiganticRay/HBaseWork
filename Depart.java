package cuit.leichao.hbase;

import java.util.List;

public class Depart
{
	public String rowKey;
	public String fPid;
	public String name;
	public List<String> childrenList;
	public int childrenNum;
	public String getRowKey()
	{
		return rowKey;
	}
	public void setRowKey(String rowKey)
	{
		this.rowKey = rowKey;
	}
	public String getfPid()
	{
		return fPid;
	}
	public void setfPid(String fPid)
	{
		this.fPid = fPid;
	}
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public List<String> getChildrenList()
	{
		return childrenList;
	}
	public void setChildrenList(List<String> childrenList)
	{
		this.childrenList = childrenList;
	}
	public int getChildrenNum()
	{
		return childrenNum;
	}
	public void setChildrenNum(int childrenNum)
	{
		this.childrenNum = childrenNum;
	}
	public Depart(String rowKey, String fPid, String name,
			List<String> childrenList, int childrenNum)
	{
		super();
		this.rowKey = rowKey;
		this.fPid = fPid;
		this.name = name;
		this.childrenList = childrenList;
		this.childrenNum = childrenNum;
	}
	public Depart()
	{
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString()
	{
		String formatString = "";
		formatString += "rowKey: " + rowKey + "\t" + "fPid: " + fPid + "\t" + "name: " + name + "\t" + "sonNum: " + childrenNum + "\t";
		for(int i = 0; i < childrenNum; i++){
			formatString += "son" + String.valueOf(i) + ": " + childrenList.get(i) + "\t";
		}
		return formatString;
	}
	
	
	
}
