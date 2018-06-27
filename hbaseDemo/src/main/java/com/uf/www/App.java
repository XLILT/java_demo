package com.uf.www;

import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;
//import java.util.Random;
//import java.net.URL;
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.Cell;  
import org.apache.hadoop.hbase.CellUtil;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.TableName;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
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
import org.apache.hadoop.hbase.util.Bytes;  
  
public class App {  
  
    public static Configuration conf;  
  
    static {
        System.setProperty("hadoop.home.dir", "/mnt/data");

        conf = HBaseConfiguration.create();  
        conf.addResource("hbase-site.xml");

        //try {
            //conf.addResource(new URL("hdfs://127.0.0.1:9000/hbase"));    
        //} catch(Exception ex){  
            //ex.printStackTrace();  
        //}       
    }  
  
    /** 
     * 创建表 
     *  
     * @param tablename 表名 
     * @param columnFamily 列族 
     * @throws MasterNotRunningException 
     * @throws ZooKeeperConnectionException 
     * @throws IOException 
     */  
    public static void createTable(String tablename, String columnFamily)  
            throws MasterNotRunningException, IOException, ZooKeeperConnectionException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Admin admin = conn.getAdmin();  
        try {  
            if (admin.tableExists(TableName.valueOf(tablename))) {  
                System.out.println(tablename + " already exists");  
            } else {  
                TableName tableName = TableName.valueOf(tablename);  
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);  
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));  
                admin.createTable(tableDesc);  
                System.out.println(tablename + " created succeed");  
            }  
        } finally {  
            admin.close();  
            conn.close();  
        }  
    }

    public static void createTable(String tablename, List<String> columnFamilyList)  
            throws MasterNotRunningException, IOException, ZooKeeperConnectionException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Admin admin = conn.getAdmin();  
        try {  
            if (admin.tableExists(TableName.valueOf(tablename))) {  
                System.out.println(tablename + " already exists");  
            } else {  
                TableName tableName = TableName.valueOf(tablename);
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);

                for(String columnFamily: columnFamilyList) {
                    tableDesc.addFamily(new HColumnDescriptor(columnFamily));      
                }
                
                admin.createTable(tableDesc);  
                System.out.println(tablename + " created succeed");  
            }  
        } finally {  
            admin.close();  
            conn.close();  
        }  
    }
      
    /** 
     * 向表中插入一条新数据 
     *  
     * @param tableName 表名 
     * @param row 行键key 
     * @param columnFamily 列族 
     * @param column 列名 
     * @param data 要插入的数据 
     * @throws IOException 
     */  
    public static void putData(String tableName, String row, String columnFamily, String column, String data)  
            throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try {  
            Put put = new Put(Bytes.toBytes(row));  
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));  
            table.put(put);  
//          System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  

    public static void putDataLoop()
            throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("session"));

        try {
            for(int i = 0; i < 100000; i++) {
                int rid = (int)(Math.random() * 100);
                int uid = (int)(Math.random() * 1000000);
                int first_heartbeat_time = (int)(System.currentTimeMillis()/1000) - 1000 - (int)(Math.random() * 1000);
                int last_heartbeat_time = (int)(System.currentTimeMillis()/1000) - (int)(Math.random() * 1000);


                Put put = new Put(Bytes.toBytes("" + rid + "_" + uid + "_" + first_heartbeat_time));
                
                put.addColumn(Bytes.toBytes("room"), Bytes.toBytes("rid"), Bytes.toBytes("" + rid));
                put.addColumn(Bytes.toBytes("room"), Bytes.toBytes("rname"), Bytes.toBytes("room" + rid));
                
                put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("uid"), Bytes.toBytes("" + uid));
                put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("uname"), Bytes.toBytes("uname" + uid));
                put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("urole"), Bytes.toBytes("" + (int)(Math.random() * 5)));
                put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("ip"), Bytes.toBytes("" + (int)(Math.random() * 100) + "." + (int)(Math.random() * 100) + "." + (int)(Math.random() * 100) + "." + (int)(Math.random() * 100)));
                put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("ctype"), Bytes.toBytes("" + (int)(Math.random() * 5)));
                put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("first_heartbeat_time"), Bytes.toBytes("" + first_heartbeat_time));
                put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("last_heartbeat_time"), Bytes.toBytes("" + last_heartbeat_time));
                put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("network_operator"), Bytes.toBytes("" + (int)(Math.random() * 5)));

                table.put(put);  
            }
        } finally {  
            table.close();  
            conn.close();  
        }  
    }
      
    /** 
     * add a column family to an existing table 
     *  
     * @param tableName table name  
     * @param columnFamily column family 
     * @throws IOException 
     */  
    public static void putFamily(String tableName, String columnFamily) throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Admin admin = conn.getAdmin();  
        try {  
            if (!admin.tableExists(TableName.valueOf(tableName))) {  
                System.out.println(tableName + " not exists");  
            } else {  
                admin.disableTable(TableName.valueOf(tableName));  
                  
                HColumnDescriptor cf1 = new HColumnDescriptor(columnFamily);  
                admin.addColumn(TableName.valueOf(tableName), cf1);  
                  
                admin.enableTable(TableName.valueOf(tableName));  
                System.out.println(TableName.valueOf(tableName) + ", " + columnFamily + " put succeed");  
            }  
        } finally {  
            admin.close();  
            conn.close();  
        }  
    }  
      
    /** 
     * 根据key读取一条数据 
     *  
     * @param tableName 表名 
     * @param row 行键key 
     * @param columnFamily 列族 
     * @param column 列名 
     * @throws IOException 
     */  
    public static void getData(String tableName, String row, String columnFamily, String column) throws IOException{  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try{  
            Get get = new Get(Bytes.toBytes(row));  
            Result result = table.get(get);  
            byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));  
            String value = new String(rb, "UTF-8");  
            System.out.println(value);  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  
  
    /** 
     * get all data of a table 
     *  
     * @param tableName table name 
     * @throws IOException 
     */  
    public static void scanAll(String tableName) throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try {  
            Scan scan = new Scan();  
            ResultScanner resultScanner = table.getScanner(scan);  
            for(Result result : resultScanner){  
                List<Cell> cells = result.listCells();  
                for(Cell cell : cells){  
                    String row = new String(result.getRow(), "UTF-8");  
                    String family = new String(CellUtil.cloneFamily(cell), "UTF-8");  
                    String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");  
                    String value = new String(CellUtil.cloneValue(cell), "UTF-8");  
                    System.out.println("[row:"+row+"],[family:"+family+"],[qualifier:"+qualifier+"],[value:"+value+"]");  
                }  
            }  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  
      
    /** 
     * delete a data by row key 
     *  
     * @param tableName table name 
     * @param rowKey row key 
     * @throws IOException 
     */  
    public static void delData(String tableName, String rowKey) throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try {  
            List<Delete> list = new ArrayList<Delete>();  
            Delete del = new Delete(rowKey.getBytes());  
            list.add(del);  
            table.delete(list);  
            System.out.println("delete record " + rowKey + " ok");  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  
      
    /** 
     * delete a column's value of a row 
     *  
     * @param tableName table name 
     * @param rowKey row key 
     * @param familyName family name 
     * @param columnName column name 
     * @throws IOException 
     */  
    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try{  
            Delete del = new Delete(Bytes.toBytes(rowKey));  
            del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));  
            List<Delete> list = new ArrayList<Delete>(1);  
            list.add(del);  
            table.delete(list);  
            System.out.println("[table:"+tableName+"],row:"+rowKey+"],[family:"+familyName+"],[qualifier:"+columnName+"]");  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  
  
    /** 
     * delete a columnFamily's all columns value of a row  
     *  
     * @param tableName table name 
     * @param rowKey row key 
     * @param familyName family name 
     * @throws IOException 
     */  
    public static void deleteFamily(String tableName, String rowKey, String familyName) throws IOException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Table table = conn.getTable(TableName.valueOf(tableName));  
        try{  
            Delete del = new Delete(Bytes.toBytes(rowKey));  
            del.addFamily(Bytes.toBytes(familyName));  
            List<Delete> list = new ArrayList<Delete>(1);  
            list.add(del);  
            table.delete(list);  
            System.out.println("[table:"+tableName+"],row:"+rowKey+"],[family:"+familyName+"]");  
        } finally {  
            table.close();  
            conn.close();  
        }  
    }  
  
    /** 
     * delete a table 
     *  
     * @param tableName table name 
     * @throws IOException 
     * @throws MasterNotRunningException 
     * @throws ZooKeeperConnectionException 
     */  
    public static void deleteTable(String tableName) throws IOException, MasterNotRunningException, ZooKeeperConnectionException {  
        Connection conn = ConnectionFactory.createConnection(conf);  
        Admin admin = conn.getAdmin();  
        try {  
            admin.disableTable(TableName.valueOf(tableName));  
            admin.deleteTable(TableName.valueOf(tableName));  
            System.out.println("delete table " + tableName + " ok");  
        } finally {  
            admin.close();  
            conn.close();  
        }  
    }  
      
    public static void main(String[] args) {  
        System.err.println("start...");  

        String tableName = "session";  

        try{
            List<String> columnFamilyList = new ArrayList<>();

            columnFamilyList.add("user");
            columnFamilyList.add("room");
            columnFamilyList.add("other");

            createTable(tableName, columnFamilyList);

            putDataLoop();

            //putDataLoop(tableName, "stu");

//          createTable(tableName + "2", "stu");  
            //createTable(tableName, "load");  
            //putData(tableName, "row_1", "load", "no", "0001");  
            //putData(tableName, "row_1", "load", "rec_date", "2016-06-03");  
            //putData(tableName, "row_1", "load", "rec_time", "09:49:00");  
            //putData(tableName, "row_1", "load", "power", "154.24");  
              
//          putData(tableName, "row_1", "stu", "stu_id", "001");  
//          putData(tableName, "row_2", "stu", "stu_id", "002");  
//          putData(tableName, "row_3", "stu", "stu_id", "003");  
//          getData(tableName, "row_1", "stu", "stu_id");  
//          delData(tableName, "row_3");  
//          scanAll(tableName);  
//          deleteTable(tableName + "2");  
//          putFamily(tableName, "score");  
//          putData(tableName, "row_4", "score", "chinese", "90");  
//          putData(tableName, "row_5", "score", "math", "91");  
//          scanAll(tableName);  
//          deleteColumn(tableName, "row_4", "score", "chinese");  
//          deleteFamily(tableName, "row_5", "score");  
            //scanAll(tableName);  
        } catch(Exception ex){  
            ex.printStackTrace();  
        }  

        System.err.println("end...");  
    }  
  
}
