import java.util.Properties
import com.siradel.dataapi.vectoranalysis.parsers.GeoParquetParser
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

var session = SparkSession.builder().appName("GeoAnalytics").master("yarn-client").config("spark.serializer", classOf[KryoSerializer].getName).config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).getOrCreate()

GeoSparkSQLRegistrator.registerAll(session)

//map kpis parquet into dataframe
val kpis_df = session.read.csv("hdfs://lfrn1cdh02.siradel.local:8020/user/atassi/NYC_Trees/2015StreetTreesCensus_TREES.csv")
kpis_df.createOrReplaceTempView("trees")

//join tables and apply group by on join fields
val count = session.sql("SELECT count(tree_id) from trees")
println(count)
