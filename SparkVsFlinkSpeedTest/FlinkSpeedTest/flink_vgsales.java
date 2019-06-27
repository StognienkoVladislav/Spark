package flink_examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class flink_vgsales {
	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		/* create table from csv */
		TableSource tableSrc = CsvTableSource.builder()
				.path("/home/vstohniienko/projects/flink_vgsales/vgsales.csv")
				.ignoreFirstLine()
				.fieldDelimiter(",")
				.ignoreParseErrors()
				.field("Rank", Types.INT)
				.field("Name", Types.STRING)
				.field("Platform", Types.STRING)
				.field("Year", Types.STRING)
				.field("Genre", Types.STRING)
				.field("Publisher", Types.STRING)
				.field("NA_Sales", Types.FLOAT)
				.field("EU_Sales", Types.FLOAT)
				.field("JP_Sales", Types.FLOAT)
				.field("Other_Sales", Types.FLOAT)
				.field("Global_Sales", Types.FLOAT)
				.build();

		tableEnv.registerTableSource("vgsales", tableSrc);

		String sql = "Select * from vgsales where Publisher='Nintendo' and " +
				"Genre='Action'";

		Table order20 = tableEnv.sqlQuery(sql);
		DataSet<Row1> nintendo_action = tableEnv.toDataSet(order20, Row1.class);
		nintendo_action.writeAsText("/home/vstohniienko/projects/flink_vgsales/nintendo_action.csv");
		env.execute("Flink vgsales");
	}

	public static class Row1
	{
		public Integer Rank;
		public String Name;
		public String Platform;
		public String Year;
		public String Genre;
		public String Publisher;
		public Float NA_Sales;
		public Float EU_Sales;
		public Float JP_Sales;
		public Float Other_Sales;
		public Float Global_Sales;

		public Row1(){}

		public String toString()
		{
			return Rank + "," + Name + "," + Platform + "," + Year + "," + Genre + "," + Publisher +
					"," + NA_Sales + "," + EU_Sales + "," + JP_Sales + "," + Other_Sales + "," + Global_Sales;
		}
	}
}