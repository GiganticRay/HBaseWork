package cuit.leichao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseHomework
{
	private Configuration config;
	private Connection conn;
	private Admin admin;

	@Before
	public void testBefore() throws IOException
	{
		try
		{
			if (config == null)
			{
				System.out.println("初始化");
				config = HBaseConfiguration.create(); 				// 取得一个数据库连接的配置参数对象
				conn = ConnectionFactory.createConnection(config); 	// 取得一个数据库连接对象			
				admin = conn.getAdmin();							// 取得一个数据库元数据操作对象
				System.out.println("初始化完成" + admin);
			}
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}

	}
	
	@After
	public void testAfter() {
		try {
			conn.close();											// 关闭连接
			System.out.println("close");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void allTest() throws Exception{
		List<Result> resultList = null;
		resultList = RowkeyFilter("LCNamespace:departTB", "^0_$");						// experiment 3.1	查询所有顶级部门
		resultList = GetRaw("LCNamespace:departTB", "departInfo", "fpid", "^0_001$");	// experiment 3.2	获取指定部门的所有子部门
		AddAptAsSonApt("LCNamespace:departTB", "0_002", "4_001");						// experiment 3.3	给父部门添加一个子部门
		DeleteHeadApt("LCNamespace:departTB", "0_002");									// experiment 3.4	删除顶级部门操作
		resultList = RowkeyFilter("LCNamespace:departTB", "^*$");						// experiment 4		格式化输出

		if (resultList != null){
			// ConsoleRowResultList(resultList);	// 普通输出
			formatOutput(resultList);				// 表格形式格式化
		}
	}
	

	/**
	 * 通过 列族、列标示 和给定的正则表达式来获取特定的行  (单列值过滤器)
	 * @param tableName 表名
	 * @param cfName	列族名
	 * @param qualifierName	列标示名
	 * @param regexString	正则表达式
	 * @throws Exception
	 */
	public List<Result> GetRaw(String tableName, String cfName, String qualifierName, String regexString) throws Exception{
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		byte[] cfCond = Bytes.toBytes(cfName);					// 设置搜索条件
		byte[] qualifierCond = Bytes.toBytes(qualifierName);
		
		Filter regexFilter = new SingleColumnValueFilter(cfCond, qualifierCond,  CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexString));
		
		Scan scan = new Scan().setStartRow(Bytes.toBytes(0)).setStopRow(Bytes.toBytes(9));	//要设置start\stop，不然 scan 会全表扫描
		scan.addColumn(cfCond, qualifierCond);					// 设置 scan 的列族:列标示 条件
		scan.setFilter(regexFilter);
		
		ResultScanner regexScanner = table.getScanner(scan);	// 结合 列族:列标示 给当前表格设置一个 scan
		
		List<Result> resultList = new ArrayList<Result>();
		for(Result res : regexScanner){
			resultList.add(res);
		}
		regexScanner.close();
		return resultList;
	}
	
	/**
	 * 通过正表达式获取指定行健 的行
	 * @param tableName		表名
	 * @param regexString	正则表达式
	 * @return 查询的 Result List
	 */
	public List<Result> RowkeyFilter(String tableName, String regexString) {
        try {
        	Table table = conn.getTable(TableName.valueOf(tableName));
            Filter regexFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexString));
            
            Scan scan = new Scan().setStartRow(Bytes.toBytes(0)).setStopRow(Bytes.toBytes(9));;
            scan.setFilter(regexFilter);          
            ResultScanner resultScan = table.getScanner(scan);
            
            List<Result> resultList = new ArrayList<Result>();
            for (Result result : resultScan) {					// 将过滤的结果放置在 list 中返回
                resultList.add(result);
            }
            resultScan.close();
            return resultList;
        } catch (IOException e) {
            e.printStackTrace();
        }
		return null;
    }
	
	/**
	 * 添加一个部门作为父部门的子部门
	 * @param tableName		表名
	 * @param headAptRowkey	父部门id
	 * @param sonAptRowkey	子部门id
	 * @return				返回是否添加成功
	 * @throws Exception
	 */
	public Boolean AddAptAsSonApt(String tableName, String headAptRowkey, String sonAptRowkey) throws Exception{
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Get headGet = new Get(Bytes.toBytes(headAptRowkey));
		Result headResult = table.get(headGet);
		
		Get sonGet = new Get(Bytes.toBytes(sonAptRowkey));
		Result sonResult = table.get(sonGet);
		
		if(headResult.isEmpty()){
			System.out.println("部门存都不存在，添加个锤子哦");
			return false;
		}
		else if(sonResult.isEmpty()){
			System.out.println("你要添加空气咩？");
			return false;
		}
		if(sonResult.getValue(Bytes.toBytes("departInfo"), Bytes.toBytes("fpid")) != null){
			System.out.println("该部门有噶父部门了哦");
			return false;
		}
		
		int sonNum = Integer.parseInt(Bytes.toString(headResult.getValue(Bytes.toBytes("sonDepartInfo"), Bytes.toBytes("son_num"))));
		
		// 给父部门添加 son_sonNum 列标示， 并且给 sonNum 加 1
		String qualifierName = "son" + String.valueOf(sonNum + 1);
		PutColumn(tableName, headAptRowkey, "sonDepartInfo", qualifierName, sonAptRowkey);		// 添加福门不子部门信息
		PutColumn(tableName, headAptRowkey, "sonDepartInfo", "son_num", String.valueOf(sonNum + 1));// 跟新父部门子部门的数量
		PutColumn(tableName, sonAptRowkey, "departInfo", "fpid", headAptRowkey);				// 添加子部门 fpid
		
		System.out.println("添加成功");
		return true;
	}
	
	/**
	 * 删除 父部门 及其之下的 子部门
	 * @param tableName
	 * @param headAptRowkey
	 * @return
	 * @throws Exception
	 */
	public Boolean DeleteHeadApt(String tableName, String headAptRowkey) throws Exception{
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Get headGet = new Get(Bytes.toBytes(headAptRowkey));
		Result headResult = table.get(headGet);
		
		int sonNum = Integer.parseInt(Bytes.toString(headResult.getValue(Bytes.toBytes("sonDepartInfo"), Bytes.toBytes("son_num"))));
		
		for(int i = 0; i < sonNum; i ++){
			String sonRowkey = Bytes.toString(headResult.getValue(Bytes.toBytes("sonDepartInfo"), Bytes.toBytes("son" + String.valueOf(i+1))));	// 循环获取其子部门行健并删除
			DeleteRow(tableName, sonRowkey);
		}
		
		DeleteRow(tableName, headAptRowkey);	// 删除完子部门之后删除他自己
		System.out.println("成功删除父部门及其子部门");
		
		return true;
	}
	
	/**
	 * 添加一个单元格
	 * @param tableName	表名
	 * @param rowKey	行健
	 * @param cfName	列族名儿
	 * @param qualifierName	列标识
	 * @param value		插入的值
	 * @throws IOException
	 */
	public void PutColumn(String tableName, String rowKey, String cfName, String qualifierName, String value) throws IOException{
		Table table = conn.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifierName), Bytes.toBytes(value));
		System.out.println("put column successfully");
		table.put(put);
	}
	
	/**
	 * 删除一个单元格
	 */
	public void DeleteColumn(String tableName, String rowKey, String cfName, String qualifierName) throws IOException{
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumns(Bytes.toBytes(cfName), Bytes.toBytes(qualifierName));
		table.delete(delete);
		System.out.println("delete this column successfully");
	}
	
	/**
	 * 删除一行
	 * @param tableName	表名
	 * @param rowKey	行健
	 * @throws Exception 
	 */
	public void DeleteRow(String tableName, String rowKey) throws Exception{
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		System.out.println("delete this row successfully");
	}
	
	/**
	 * 格式化输出
	 * @param resultList
	 */
	public void formatOutput(List<Result> resultList){
		byte[] cfCond = Bytes.toBytes("departInfo");
		byte[] qualifierCond = Bytes.toBytes("name");
		
		List<Depart> departList = new ArrayList<Depart>();
		
		for (Result result : resultList) {
			Depart departItem = new Depart();
			departItem.rowKey = Bytes.toString(result.getRow());
			departItem.childrenList = new ArrayList<String>();
            for (Cell cell : result.rawCells()) {
            	if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("fpid")){
            		departItem.fPid = Bytes.toString(CellUtil.cloneValue(cell));
            	}
            	if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("name")){
            		departItem.name = Bytes.toString(CellUtil.cloneValue(cell));
            	}
            	if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("son_num")){
            		departItem.childrenNum =  Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell)));
            	}
            	if(Pattern.matches("^son*[0-9]$", Bytes.toString(CellUtil.cloneQualifier(cell)))){
            		departItem.childrenList.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
            	}
            }           
            System.out.println(departItem);
        }
		System.out.println();
	}
	
	/**
	 * 输出 result List
	 * @param resultList
	 */
	public void ConsoleRowResultList(List<Result> resultList){
		byte[] cfCond = Bytes.toBytes("departInfo");
		byte[] qualifierCond = Bytes.toBytes("name");
		
		for (Result result : resultList) {
            for (Cell cell : result.rawCells()) {
                System.out.println(  
                        "Rowkey-->"+Bytes.toString(result.getRow())+"  "+  
                        "Familiy:Quilifier-->"+""+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                        "Value-->"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
		System.out.println();
	}
}
