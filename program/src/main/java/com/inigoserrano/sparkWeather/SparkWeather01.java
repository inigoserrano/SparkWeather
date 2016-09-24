package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWeather01 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather01").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		final String path = "/Users/inigo/git/SparkWeatherJava/program/src/main/resources/EasyWeather.txt";
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);

		datosMeteorologicos.printSchema();

		datosMeteorologicos.write().json("/Users/inigo/git/SparkWeatherJava/program/src/main/resources/json");

		spark.close();
	}

}
