package flink_examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.math.BigDecimal;

public class flink_large_data {
	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		/* create table from csv */
		TableSource tableSrc = CsvTableSource.builder()
				.path("/home/vstohniienko/projects/gener_datasets/data_16_gb.csv")
				.ignoreFirstLine()
				.fieldDelimiter(",")
				.ignoreParseErrors()
				.field("UUIDs", Types.STRING)
				.field("random_float_1", Types.BIG_DEC)
				.field("random_float_2", Types.BIG_DEC)
				.field("random_int", Types.INT)
				.build();

		tableEnv.registerTableSource("sample_large", tableSrc);
		String sql = "Select * from sample_large where random_float_1 > 40 and random_float_2 < 20 and random_int > 400";

		Table order20 = tableEnv.sqlQuery(sql);

		DataSet<Row1> nintendo_action = tableEnv.toDataSet(order20, Row1.class);
		nintendo_action.writeAsText("/home/vstohniienko/projects/flink_vgsales/sample_result.csv");
		env.execute("SQL API Example");
	}

	public static class Row1
	{
		public String UUIDs;
		public BigDecimal random_float_1;
		public BigDecimal random_float_2;
		public Integer random_int;

		public Row1(){}

		public String toString()
		{
			return UUIDs + "," + random_float_1 + "," + random_float_2 + "," + random_int;
		}
	}
}