package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkWeather05 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather05").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(spark);

		DataFrame datosMeteorologicos = obtenerDatos(sqlContext);
		datosMeteorologicos = datosMeteorologicos.select("Time", "Indoor_Temperature");
		datosMeteorologicos = datosMeteorologicos.filter("Indoor_Temperature < 10");

		datosMeteorologicos.write().json("hdfs://localhost:9000/user/iserrano/sparkWeather/");

		spark.close();
	}

	private static DataFrame obtenerDatos(final SQLContext sqlContext) {
		final String path = "/home/iserrano/workspace/SparkWeather/src/main/resources/EasyWeather.txt";
		final DataFrame datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}

}
