package com.inigoserrano.sparkWeather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class SparkWeather04 {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather04").setMaster("local");
		sparkConf.set("es.nodes", "172.17.0.2:9200");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(spark);

		DataFrame datosMeteorologicos = obtenerDatos(sqlContext);
		datosMeteorologicos = datosMeteorologicos.select("Time", "Indoor_Temperature");
		datosMeteorologicos = datosMeteorologicos.filter("Indoor_Temperature < 10");

		JavaEsSparkSQL.saveToEs(datosMeteorologicos, "sparkweather/datosmeteorologicos");

		spark.close();
	}

	private static DataFrame obtenerDatos(final SQLContext sqlContext) {
		final String path = "/home/iserrano/workspace/SparkWeather/src/main/resources/EasyWeather.txt";
		final DataFrame datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}

}
