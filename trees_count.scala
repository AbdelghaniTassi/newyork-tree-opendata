import java.util.Properties
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession


var session = SparkSession.builder().appName("GeoAnalytics").master("yarn-client").config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()

//map kpis parquet into dataframe
val trees_df = session.read.format("csv").option("header", "true").load("/home/abdelghani/Downloads/2015_Street_Tree_Census_-_Tree_Data.csv")
println(trees_df.count())
