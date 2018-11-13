package com.saurzcode.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.sql.functions;
public class SparkTranformationData  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SparkSession spark;
	JavaSparkContext context ;
	private SparkTranformationData() {
		spark = SparkSession
			      .builder().master("local[2]")
			      .appName("Java Spark SQL basic example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		context = new JavaSparkContext(spark.sparkContext());
	}
	
	public static void main(String args[]) {
		try {
		System.setProperty("hadoop.home.dir", "E:\\Hadoop\\");
		SparkTranformationData transform = new SparkTranformationData();
	
		 //Reading the data from kafka topic
		 JavaStreamingContext sContext = new JavaStreamingContext(transform.context,Seconds.apply(10));
		 
			Map<String, Object> kafkaParams = new HashMap<>();
			kafkaParams.put("bootstrap.servers", "localhost:9092");
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream2");
//			kafkaParams.put("auto.offset.reset", "latest");
			kafkaParams.put("auto.offset.reset", "earliest");
			kafkaParams.put("enable.auto.commit", false);
			Map<String, String> kafkaParams1 = new HashMap<>();
			kafkaParams.put("'kafka.bootstrap.servers", "localhost:9092");
			Dataset<Row>  data = transform.spark.readStream()
			  .format("kafka")
			  .option("kafka.bootstrap.servers", "localhost:9092")
			  .option("truncate", "false")
			  .option("subscribe", "agentnotreadydetail")
			  .option("startingoffsets", "earliest")
			  .load();
		
			StructField[] field=new StructField[] { new StructField("Completed_Indicator", DataTypes.StringType, true,Metadata.empty())
					,new StructField("CreateDt", DataTypes.LongType, true,Metadata.empty()),
					new StructField("Last_Update_Timestamp", DataTypes.StringType, true,Metadata.empty()),
					new StructField("NotReadyEndDt", DataTypes.LongType, true,Metadata.empty()),
					new StructField("NotReadyStartDt", DataTypes.LongType, true,Metadata.empty()),
					new StructField("ParkFlag", DataTypes.LongType, true,Metadata.empty()),
					new StructField("SourceDatabaseName", DataTypes.StringType, true,Metadata.empty()),
					new StructField("SourceServerName", DataTypes.StringType, true,Metadata.empty()),
					new StructField("UpdateDt", DataTypes.LongType, true,Metadata.empty()),
					new StructField("User_Id", DataTypes.StringType, true,Metadata.empty())};
			StructType schema = new StructType(field);
		Dataset<Row>  streamingDf1 = data.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").select(functions.from_json(functions.col("value"),schema)) ;
				 
		streamingDf1.writeStream().format("console").start().awaitTermination();;
System.out.println("outside await termination");
		Dataset<String> datadf = streamingDf1.toJSON();
	
		datadf.show();
		
	//	
		
		
	
		
//			Collection<String> topics = Arrays.asList("test1");
			
//			 JavaDStream<String> data  = KafkaUtils.createDirectStream(
//					    sContext,
//					    LocationStrategies.PreferConsistent(),
//					    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//					  ).map(record -> record.value().trim());
//			 data.foreachRDD( line -> line.foreach( r -> System.out.println(r)));
//			 data.print();
//			 DailerTranformData transformdata = new DailerTranformData();
//			 transformdata.setSpark(transform.spark);
//			 data.foreachRDD(transformdata);
//			 data.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//				@Override
//				public void call(JavaRDD<String> arg0) throws Exception {
//					transform.transformData(arg0);
//				}
//				
//			});
			 sContext.start();
			 sContext.awaitTermination();
	}catch(Exception e) {
		e.printStackTrace();
	}
	}
	
	//Processing the data by using spark sql(Aggregations and tranformations
	private void transformData(JavaRDD<String> dataRDD) {
		Dataset<Row> df  = spark.read().json(dataRDD);
	//	JavaRDD<MarketData> empData = dataRDD.map(line -> new Gson().fromJson(line, MarketData.class));
	//	Dataset<Row> df = spark.createDataFrame(empData, MarketData.class);
		df.show();
		//Registering the data as table
		 df.createOrReplaceTempView("market");	 
		 //sum of the salary by using dept
	//	 Dataset<Row> dfsalary = spark.sql("select sum(sal) as salary,TitleName from emp group by dept");
		 //order the data by using empId
		 Dataset<Row> dfEmpid = spark.sql("select * from market order by user_id");
		 //Sending the final data to kafka in json format
		
		 dfEmpid.toJSON().foreach(new ForeachFunction<String>() {
				private static final long serialVersionUID = 2L;
				public void call(String arg0) throws Exception {
					System.out.println(arg0);
					//sendDatatoKafka(arg0);
				}
			});
		 
		
	}
	private void sendDatatoKafka(String data) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
	props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	            LongSerializer.class.getName());
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	        StringSerializer.class.getName());
	
	KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
	producer.send(new ProducerRecord<String, String>("test", "Welcome from java programme"));
	producer.flush();
	}
	
}
class Employee{
	private String empName;
	private int empId;
	private int sal;
	private String dept;
	public String getEmpName() {
		return empName;
	}
	public void setEmpName(String empName) {
		this.empName = empName;
	}
	public int getEmpId() {
		return empId;
	}
	public void setEmpId(int empId) {
		this.empId = empId;
	}
	public int getSal() {
		return sal;
	}
	public void setSal(int sal) {
		this.sal = sal;
	}
	public String getDept() {
		return dept;
	}
	public void setDept(String dept) {
		this.dept = dept;
	}
 class MarketData{
	 private String market;
	 private String workgroup;
	 public String getMarket() {
		return market;
	}
	public void setMarket(String market) {
		this.market = market;
	}
	public String getWorkgroup() {
		return workgroup;
	}
	public void setWorkgroup(String workgroup) {
		this.workgroup = workgroup;
	}
	public int getWorkgroup_id() {
		return workgroup_id;
	}
	public void setWorkgroup_id(int workgroup_id) {
		this.workgroup_id = workgroup_id;
	}
	public String getSegment() {
		return segment;
	}
	public void setSegment(String segment) {
		this.segment = segment;
	}
	public String getSegment_desc() {
		return segment_desc;
	}
	public void setSegment_desc(String segment_desc) {
		this.segment_desc = segment_desc;
	}
	public int getUser_id() {
		return user_id;
	}
	public void setUser_id(int user_id) {
		this.user_id = user_id;
	}
	private int workgroup_id;
	 private String segment;
	 private String segment_desc;
	 private int user_id;
	 
 }
}
