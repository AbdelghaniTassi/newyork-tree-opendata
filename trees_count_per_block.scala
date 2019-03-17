import java.util.Properties
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession


var session = SparkSession.builder().appName("GeoAnalytics").master("yarn-client").config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()

//map kpis parquet into dataframe
val trees_df = session.read.format("csv").option("header", "true").load("/home/abdelghani/Downloads/2015_Street_Tree_Census_-_Tree_Data.csv")

trees_df.registerTempTable("trees")

val trees_groupby_health_df = session.sql("SELECT block_id, count(tree_id) FROM trees GROUP BY(block_id)")

trees_groupby_health_df.show()
