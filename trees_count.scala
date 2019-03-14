import java.util.Properties
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession


var session = SparkSession.builder().appName("GeoAnalytics").master("yarn-client").config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()

//map kpis parquet into dataframe
val trees_df = session.read.format("csv").option("header", "true").load("hdfs://lfrn1cdh02.siradel.local:8020/user/atassi/NYC_Trees/2015StreetTreesCensus_TREES.csv")
println(trees_df.count())
