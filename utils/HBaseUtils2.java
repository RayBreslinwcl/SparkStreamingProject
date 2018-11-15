package StreamingProject.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by Administrator on 2018/9/25.
 * HBase操作工具类:Java工具类建议采用单列模式封装
 */
public class HBaseUtils2 {
    HBaseAdmin admin=null;
    Configuration configuration=null;

    /**
     * 私有改造方法
     */
    private HBaseUtils2(){
        configuration=new Configuration();
        configuration.set("hbase.rootdir","hdfs://bigdata.ibeifeng.com:8020/hbase");
        configuration.set("hbase.zookeeper.quorum","bigdata.ibeifeng.com:2181");

        try {
            admin =new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static HBaseUtils2 instance=null;

    public static synchronized HBaseUtils2 getInstance(){
        if (null==instance){
            instance=new HBaseUtils2();
        }
        return instance;
    }

    public HTable getTable(String tableName){
        HTable table=null;

        try {
            table =new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一列
     * @param tableName Hbase表名
     * @param rowkey rowkey
     * @param cf HBase的columnnfamily
     * @param column HBase列
     * @param value 写入HBase表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        HTable table=getTable(tableName);

        Put put=new Put(Bytes.toBytes(rowkey));

        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
    }
    public static void main(String[] args) {
//        HTable table= HBaseUtils2.getInstance().getTable("imooc_course_clickcount");
//        System.out.println(table.getName().getNameAsString());

        String tablename="imooc_course_clickcount";
        String rowkey="20171111_88";
        String cf="info";
        String column="click_count";
        String value="2";

        try {
            HBaseUtils2.getInstance().put(tablename,rowkey,cf,column,value);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }

    }
}
