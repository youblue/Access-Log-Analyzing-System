package spark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class GetTopShoppingHour {
	static class Transformer implements Function<String, Row> {
        /**
		 * Spark program to find out which hour US people do most shopping
		 */
		private static final long serialVersionUID = 1L;
		Pattern linePattern1 = Pattern.compile("(.*?) .*?\\[(.*?)\\].*?&url=(.*?)(?:&|%26).*");
        static LookupService cl;
        static Object lock = new Object();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        
        @SuppressWarnings("deprecation")
		@Override
        public Row call(String line) throws Exception {
            Matcher m1 = linePattern1.matcher(line);
            String ip = null;
            String dt = null;
            if (m1.find()) {
                ip = m1.group(1);
                dt = m1.group(2);
            }
            synchronized(lock) {
                if (cl == null) {
                    cl = new LookupService(SparkFiles.get("GeoLiteCity.dat"),
                            LookupService.GEOIP_MEMORY_CACHE );
                }
            }
            dt = dt.split(" ")[0];
            int hour = sdf.parse(dt).getHours();
            Location loc = cl.getLocation(ip);
            return RowFactory.create(ip, loc!=null?loc.countryCode:null, hour);
        }
    }
    
	public static void main(String[] args) throws IOException {
		SparkSession spark = SparkSession
                .builder()
                .appName(GetTopShoppingHour.class.getName())
                .getOrCreate();
		JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		/**
		 * Read log file from HDFS
		 */
		/*
		JavaRDD<Row> accessLogRDD = context.textFile("access_log_sample")
				 .filter(line -> line.matches(".*&url=(https:|http:|https%3A|http%3A)//www.amazon.com/.*"))
		         .map(new Transformer());
		*/
		/**
		 * Read log file from AWS S3
		 */
		
		JavaRDD<Row> accessLogRDD = context.textFile("s3a://cloudacl/access_log/localhost_access_log.*")
				.filter(line -> line.matches(".*&url=(https:|http:|https%3A|http%3A)//www.amazon.com/.*"))
		        .map(new Transformer());

		List<StructField> accessLogFields = new ArrayList<>();
		accessLogFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
		accessLogFields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
		accessLogFields.add(DataTypes.createStructField("hour", DataTypes.IntegerType, true));
        StructType accessLogType = DataTypes.createStructType(accessLogFields);

        Dataset<Row> accessLogDf = spark.createDataFrame(accessLogRDD, accessLogType)
                .where("country = 'US'");
        
        /**
		 * Run on HDFS
		 */
        /*
		accessLogDf
		    .groupBy("hour")
		    .agg(functions.count("*").as("c"))
		    .sort(functions.desc("c"))
		    .write()
		    .csv("output");
		*/
		/**
		 * Run on AWS EMR
		 */
		
		accessLogDf
	    	.groupBy("hour")
	    	.agg(functions.count("*").as("c"))
	    	.sort(functions.desc("c"))
	    	.coalesce(1)
	    	.write()
	    	.csv("s3://bittiger-cs502-zws/sparkOutput");
	    
	}
}
