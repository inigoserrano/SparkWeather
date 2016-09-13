package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkWeather02 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather02").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(spark);

		DataFrame datosMeteorologicos = obtenerDatos(sqlContext);
		datosMeteorologicos.registerTempTable("datos");
		datosMeteorologicos = sqlContext.sql("select Time, Indoor_Temperature from datos where Indoor_Temperature < 10");

		salvarDatos(datosMeteorologicos);
		spark.close();
	}

	private static void salvarDatos(final DataFrame datosMeteorologicos) {
		datosMeteorologicos.write().json("/home/iserrano/workspace/SparkWeather/src/main/resources/json");
	}

	private static DataFrame obtenerDatos(final SQLContext sqlContext) {
		final String path = "/home/iserrano/workspace/SparkWeather/src/main/resources/EasyWeather.txt";
		final DataFrame datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}

}
