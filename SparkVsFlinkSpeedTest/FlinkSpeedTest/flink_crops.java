package flink_examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class flink_crops {
	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		/* create table from csv */
		TableSource tableSrc = CsvTableSource.builder()
				.path("/home/vstohniienko/projects/flink_vgsales/fao_data_crops_data.csv")
				.ignoreFirstLine()
				.fieldDelimiter(",")
				.ignoreParseErrors()
				.field("country_or_area", Types.STRING)
				.field("element_code", Types.INT)
				.field("element_value", Types.STRING)
				.field("year", Types.INT)
				.field("crop_unit", Types.STRING)
				.field("value", Types.STRING)
				.field("value_footnotes", Types.STRING)
				.field("category", Types.STRING)
				.build();
		tableEnv.registerTableSource("crops", tableSrc);
		String sql = "Select * from crops where element_code < 80 and element_value = 'Area Harvested' and value_footnotes = 'A '";

		Table order20 = tableEnv.sqlQuery(sql);

		DataSet<Row1> nintendo_action = tableEnv.toDataSet(order20, Row1.class);
		nintendo_action.writeAsText("/home/vstohniienko/projects/flink_vgsales/crops_data.csv");
		env.execute("SQL API Example");
	}

	public static class Row1
	{
		public String country_or_area;
		public Integer element_code;
		public String element_value;
		public Integer year;
		public String crop_unit;
		public String value;
		public String value_footnotes;
		public String category;

		public Row1(){}

		public String toString()
		{
			return country_or_area + "," + element_code + "," + element_value + "," + year + "," + crop_unit + "," + value +
					"," + value_footnotes + "," + category;
		}
	}
}