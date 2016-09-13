package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkWeather01 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather01").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(spark);

		final String path = "/home/iserrano/workspace/SparkWeather/src/main/resources/EasyWeather.txt";
		final DataFrame datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);

		datosMeteorologicos.printSchema();

		datosMeteorologicos.write().json("/home/iserrano/workspace/SparkWeather/src/main/resources/json");

		spark.close();
	}

}
