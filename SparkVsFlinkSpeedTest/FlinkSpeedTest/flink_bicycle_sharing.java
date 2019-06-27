package flink_examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class flink_bicycle_sharing {
	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		/* create table from csv */
		TableSource tableSrc = CsvTableSource.builder()
				.path("bicycle_sharing.csv")
				.ignoreFirstLine()
				.fieldDelimiter(",")
				.ignoreParseErrors()
				.field("trip_id", Types.INT)
				.field("year", Types.INT)
				.field("month", Types.INT)
				.field("week", Types.INT)
				.field("day", Types.INT)
				.field("hour", Types.INT)
				.field("usertype", Types.STRING)
				.field("gender", Types.STRING)
				.field("starttime", Types.STRING)
				.field("stoptime", Types.STRING)
				.field("tripduration", Types.STRING)
				.field("temperature", Types.FLOAT)
				.field("events", Types.STRING)
				.field("from_station_id", Types.INT)
				.field("from_station_name", Types.STRING)
				.field("latitude_start", Types.STRING)
				.field("longitude_start", Types.STRING)
				.field("dpcapacity_start", Types.FLOAT)
				.field("to_station_id", Types.INT)
				.field("to_station_name", Types.STRING)
				.field("latitude_end", Types.STRING)
				.field("longitude_end", Types.STRING)
				.field("dpcapacity_end", Types.FLOAT)
				.build();

		tableEnv.registerTableSource("bicycle_sharing", tableSrc);

		String sql = "Select * from bicycle_sharing where to_station_id = 383";

		Table flink_bicycle_sharing = tableEnv.sqlQuery(sql);

		DataSet<RowsTemplete> flink_bicycle = tableEnv.toDataSet(flink_bicycle_sharing, RowsTemplete.class);
		flink_bicycle.writeAsText("flink_bicycle_sharing_data.csv");

		env.execute("Flink Bicycle Sharing");
	}

	public static class RowsTemplete
	{
		public Integer trip_id;
		public Integer year;
		public Integer month;
		public Integer week;
		public Integer day;
		public Integer hour;
		public String usertype;
		public String gender;
		public String starttime;
		public String stoptime;
		public String tripduration;
		public Float temperature;
		public String events;
		public Integer from_station_id;
		public String from_station_name;
		public String latitude_start;
		public String longitude_start;
		public Float dpcapacity_start;
		public Integer to_station_id;
		public String to_station_name;
		public String latitude_end;
		public String longitude_end;
		public Float dpcapacity_end;

		public RowsTemplete(){}

		public String toString()
		{
			return trip_id + "," + year + "," + month + "," + week + "," + day + "," + hour + "," + usertype + "," +
					gender + "," + starttime + "," + stoptime + "," + tripduration + "," + temperature + "," + events +
					"," + from_station_id + "," + from_station_name + "," + latitude_start + "," + longitude_start + ","
					+ dpcapacity_start + "," + to_station_id + "," + to_station_name + "," + latitude_end + ","
					+ longitude_end + "," + dpcapacity_end ;
		}
	}
}