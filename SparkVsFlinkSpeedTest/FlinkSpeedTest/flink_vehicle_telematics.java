package flink_examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class flink_vehicle_telematics {
	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		/* create table from csv */
		TableSource tableSrc = CsvTableSource.builder()
				.path("/home/vstohniienko/projects/flink_vgsales/v2.csv")
				.ignoreFirstLine()
				.fieldDelimiter(",")
				.ignoreParseErrors()
				.field("tripID", Types.INT)
				.field("deviceID", Types.STRING)
				.field("timeStamp", Types.STRING)
				.field("accData", Types.STRING)
				.field("gps_speed", Types.FLOAT)
				.field("battery", Types.FLOAT)
				.field("cTemp", Types.FLOAT)
				.field("dtc", Types.FLOAT)
				.field("eLoad", Types.FLOAT)
				.field("iat", Types.FLOAT)
				.field("imap", Types.FLOAT)
				.field("kpl", Types.FLOAT)
				.field("maf", Types.FLOAT)
				.field("rpm", Types.FLOAT)
				.field("speed", Types.FLOAT)
				.field("tAdv", Types.FLOAT)
				.field("tPos", Types.FLOAT)
				.build();

		tableEnv.registerTableSource("vehicle", tableSrc);

		String sql = "Select * from vehicle where speed > 30 and rpm > 1300 and maf > 20 and iat > 40";

		Table kindle_info = tableEnv.sqlQuery(sql);
		DataSet<Row1> kindle_reviews = tableEnv.toDataSet(kindle_info, Row1.class);
		kindle_reviews.writeAsText("/home/vstohniienko/projects/flink_vgsales/vehicle_telematics.csv");
		env.execute("Flink vehicle telematics");

	}

	public static class Row1
	{
		public Integer tripID;
		public String deviceID;
		public String timeStamp;
		public String accData;
		public Float gps_speed;
		public Float battery;
		public Float cTemp;
		public Float dtc;
		public Float eLoad;
		public Float iat;
		public Float imap;
		public Float kpl;
		public Float maf;
		public Float rpm;
		public Float speed;
		public Float tAdv;
		public Float tPos;

		public Row1(){}

		public String toString()
		{
			return tripID + "," + deviceID + "," + timeStamp + "," + accData + "," + gps_speed + "," + battery +
					"," + cTemp + "," + dtc + "," + eLoad + "," + iat + "," + imap + "," + kpl + "," + maf + "," + rpm +
					"," + speed + "," + tAdv + "," + tPos;
		}
	}
}