import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

/**
 *
 * @author Rafael Reyes Lopez
 * @email rafareyeslopez@gmail.com
 * @date 2021-03-03
 *
 */
public class Main {

	public static void main(final String[] args) throws IOException {

		SimpleDateFormat formatTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

		// Set logging level to WARN messages
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		// Set Hadoop directory
		System.setProperty("hadoop.home.dir", "C:\\Users\\Rafa\\eclipse-workspace\\hadoop\\");

		// Load the data from database
		SparkConf conf = (new SparkConf()).setAppName("startingSpark").setMaster("local[*]")
				.set("spark.executor.cores", "5").set("spark.executor.memory", "8g");

		conf.set("spark.cassandra.connection.host", "68.183.46.218");
		// conf.set("spark.cassandra.input.consistency.level", "ALL");
		conf.set("spark.cassandra.input.consistency.level", "LOCAL_ONE");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Spark calculation of average days subscribed").config(conf)
				.master("local[*]").config("spark.executor.cores", "5").getOrCreate();

//		Calendar calendarFrom = Calendar.getInstance();
//		calendarFrom.set(2017, 3, 2);
		Calendar calendarTo = Calendar.getInstance();
		calendarTo.set(2022, 1, 1);

		CassandraTableScanJavaRDD<CassandraRow> select = CassandraJavaUtil.javaFunctions(spark.sparkContext())
				.cassandraTable("click", "clicks")
				.select(new String[] { "click_uuid", "timestamp", "application_id", "campaign_id", "country",
						"country_id", "date", "ip", "isp", "landing_page_version", "network_click", "network_id",
						"premium_service_id", "publisher_id", "query_string", "request_header_info", "service",
						"sub_publisher_id", "subpublisher_id" })
				.limit(2L);

		System.out.println("EN EL SELECT HAY " + select.count());

		JavaRDD<CassandraRow> filter = select.filter(
				row -> (row.getDate("timestamp") != null && row.getDate("timestamp").before(calendarTo.getTime())));

		System.out.println("EN EL FILTRO HAY " + filter.count());

		Encoder<Click> clickEncoder = Encoders.bean(Click.class);

		// Dataset<Person> personDS = sqlContext.createDataset(select.rdd(),
		// Encoders.bean(Click.class));

		JavaRDD<Click> map = filter.map(row -> {
			try {

				return new Click(row.getUUID("click_uuid").toString(),
						row.getDate("timestamp") != null ? row.getDate("timestamp") : null,
						row.getInt("application_id"), row.getInt("campaign_id"), row.getString("country"),
						row.getInt("country_id"), row.getDate("date") != null ? row.getDate("date") : null,
						row.getString("ip"), row.getString("isp"), row.getString("landing_page_version"),
						row.getString("network_click"), row.getInt("network_id"), row.getInt("premium_service_id"),
						row.getString("publisher_id"), row.getString("query_string"),
						row.getString("request_header_info"), row.getString("service"),
						row.getString("sub_publisher_id"), row.getString("subpublisher_id"));
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(row.toString());
				return null;
			}
		});

		Dataset<Click> createDataset = spark.createDataset(map.rdd(), clickEncoder);

//		Dataset<Row> fill = createDataset.na().fill("rafa");
//		Dataset<Row> fill2 = fill.na().fill(0);
//		fill2.printSchema();

		BufferedWriter writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/clicks_old.csv"));

		// createDataset.collectAsList().forEach(System.out::println);

		createDataset.collectAsList().forEach(row -> {
			try {
				String uuid = URLEncoder.encode(row.getClickUuid(), StandardCharsets.UTF_8);
				String timestamp = formatTimestamp.format(row.getTimestamp());
				String applicationId = URLEncoder.encode(
						row.getApplicationId() == null ? "" : row.getApplicationId().toString(),
						StandardCharsets.UTF_8);
				String country = URLEncoder.encode(row.getCountry() == null ? "" : row.getCountry(),
						StandardCharsets.UTF_8);

				String date = formatDate.format(row.getTimestamp());

				// String date = formatDate.format(row.getDate() == null ? "" : row.getDate());
				String ip = URLEncoder.encode(row.getIp() == null ? "" : row.getIp(), StandardCharsets.UTF_8);
				String isp = URLEncoder.encode(row.getIsp() == null ? "" : row.getIsp(), StandardCharsets.UTF_8);
				String landingPageVersion = URLEncoder.encode(
						row.getLandingPageVersion() == null ? "" : row.getLandingPageVersion(), StandardCharsets.UTF_8);
				String networkClick = URLEncoder.encode(row.getNetworkClick() == null ? "" : row.getNetworkClick(),
						StandardCharsets.UTF_8);
				String publisherId = URLEncoder.encode(row.getPublisherId() == null ? "" : row.getPublisherId(),
						StandardCharsets.UTF_8);
				String queryString = URLEncoder.encode(row.getQueryString() == null ? "" : row.getQueryString(),
						StandardCharsets.UTF_8);
				String header = URLEncoder.encode(row.getRequestHeaderInfo() == null ? "" : row.getRequestHeaderInfo(),
						StandardCharsets.UTF_8);
				String service = URLEncoder.encode(row.getService() == null ? "" : row.getService(),
						StandardCharsets.UTF_8);
				String sub_publisherId = URLEncoder.encode(
						row.getSub_publisherId() == null ? "" : row.getSub_publisherId(), StandardCharsets.UTF_8);
				String subPublisherId = URLEncoder
						.encode(row.getSubpublisherId() == null ? "" : row.getSubpublisherId(), StandardCharsets.UTF_8);

				writer.write(uuid + "," + timestamp + "," + applicationId + "," + row.getCampaignId() + "," + country
						+ "," + row.getCountryId() + "," + date + "," + ip + "," + isp + "," + landingPageVersion + ","
						+ networkClick + "," + row.getNetworkId() + "," + row.getPremiumServiceId() + "," + publisherId
						+ "," + queryString + "," + header + "," + service + "," + sub_publisherId + ","
						+ subPublisherId + "," + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println(row.toString());
				e.printStackTrace();

			}
		});

		writer.close();

		// spark.close();
		// sc.close();

		// fill2.write().csv("/home/rafael/clicks2");
		// createDataset.write().format("csv")..save("/home/rafael/clicks2");

		// createDataset.write().csv("/home/rafael/clicks2");

		// map.saveAsTextFile("/home/rafael/clicks/");

		// map. .write.format("text").save("/path/to/save");

		// map.collect().forEach(System.out::println);

		// javaBeanDS.show();

//		String jsonPath = "data/employees.json";
//		Dataset<Employee> ds = spark.read().json(jsonPath).as(employeeEncoder);

		// write dataset to JSON file
//		ds.write().json("data/out_employees/");

		// JavaRDD<String> clickLogs = sc.textFile("clicks_start_20190101.csv");
		// change
		// this.

		// accessLogs.saveAsTextFile("/home/rafael");

//		BufferedWriter writer = new BufferedWriter(new FileWriter("clicks_start_20190101.csv"));
//		filter.collect().writer.close();
//		spark.close();
//		sc.close();

//		// Convert the Dataset into a JavaRDD
//		final JavaRDD<Tuple3<String, Timestamp, Timestamp>> usersMap = load.toJavaRDD()
//				.map(row -> new Tuple3<>(row.getString(0), row.getTimestamp(1), row.getTimestamp(2)));
//
//		// Get for each user how many days has been into service
//		final JavaPairRDD<String, Long> mapToPair = usersMap.mapToPair(row -> new Tuple2<>(row._1(),
//				TimeUnit.MILLISECONDS.toDays(
//
//						row._3() == null ? Calendar.getInstance().getTimeInMillis() - row._2().getTime()
//								: row._3().getTime() - row._2().getTime()
//
//				)));
//
//		// Get the total of users for calculate average
//		final long count = mapToPair.count();
//
//		// Get the sum of days in service
//		final Long reduce = mapToPair.values().reduce((value1, value2) -> value1 + value2);
//
//		// Print out the average
//		System.out.println("Average days subscribed " + reduce / count);

	}

}