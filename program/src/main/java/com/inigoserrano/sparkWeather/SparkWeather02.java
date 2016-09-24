package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWeather02 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather02").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = obtenerDatos(sqlContext);
		final Dataset<DatosMeteorologicos> as = datosMeteorologicos.as(Encoders.bean(DatosMeteorologicos.class));

		as.createOrReplaceTempView("datos");
		datosMeteorologicos = sqlContext.sql("select Time, Indoor_Temperature from datos where Indoor_Temperature < 10");

		salvarDatos(datosMeteorologicos);
		spark.close();
	}

	private static void salvarDatos(final Dataset<Row> datosMeteorologicos) {
		datosMeteorologicos.write().json("/Users/inigo/git/SparkWeatherJava/program/src/main/resources/json");
	}

	private static Dataset<Row> obtenerDatos(final SparkSession sqlContext) {
		final String path = "/Users/inigo/git/SparkWeatherJava/program/src/main/resources/EasyWeather.txt";
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}

}
