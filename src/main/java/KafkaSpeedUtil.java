import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Properties;

/**
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试
 * Created by zhisheng on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
public class KafkaSpeedUtil {
    public static final String broker_list = "101.35.166.55:9092";
    public static final String topic = "speed";  // kafka topic，Flink 程序中需要和这个统一

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://101.35.166.55:3306/location_flink";

    static final String USER = "root";
    static final String PASS = "123456";

    public static void mysqlToKafka() throws InterruptedException{
        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM terminal_phone_11641549033";
            ResultSet rs = stmt.executeQuery(sql);

            // 展开结果集数据库
            while(rs.next()){
                Properties props = new Properties();
                props.put("bootstrap.servers", broker_list);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
                KafkaProducer producer = new KafkaProducer<String, String>(props);

                SpeedEvent speed = new SpeedEvent();
                speed.setSpeed(rs.getInt("speed"));
                speed.setLatitude(rs.getString("latitude"));
                speed.setLongitude(rs.getString("longitude"));
                speed.setTerminal_phone("terminal_phone");

                ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(speed));
                producer.send(record);
                System.out.println("发送数据: " + GsonUtil.toJson(speed));

                producer.flush();

                Thread.sleep(100);

            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

//    public static void writeToKafka() throws InterruptedException {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", broker_list);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
//        KafkaProducer producer = new KafkaProducer<String, String>(props);
//
//        Random random = new Random();
//        int k = random.nextInt(100)+1;
//        SpeedEvent speed = new SpeedEvent();
//        speed.setSpeed(k);
//
//
//        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(speed));
//        producer.send(record);
//        System.out.println("发送数据: " + GsonUtil.toJson(speed));
//
//        producer.flush();
//    }



    public static void main(String[] args) throws InterruptedException {
//        while (true) {
//            Thread.sleep(3000);
//            writeToKafka();
//        }
        mysqlToKafka();
    }
}
