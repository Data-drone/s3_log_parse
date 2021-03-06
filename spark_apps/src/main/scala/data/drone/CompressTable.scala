package data.drone

/* Spark app to compress my parquet data down */
import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{to_timestamp, year, month, dayofmonth, col, hour, to_date}

object CompressTable {

    def main(args: Array[String]) {

        //jceks file in cluster config not working yet
        // manually setting in spark shell works
        /*start session*/
        val spark = SparkSession
            .builder()
            .appName("CompressApp")
            .config("spark.executor.cores", "2")
            .config("spark.num.executors", "10")
            .config("spark.executor.memory", "2g")
            .getOrCreate()
        
        import spark.implicits._

        /* Get a file list to run quick exps */
        val rawS3Dir = "s3a://blaws3logsorganised/RawParquet_test2"
        //val fileSystem = FileSystem.get(URI.create(rawS3Dir), new Configuration())
        val fileSystem = FileSystem.get(URI.create(rawS3Dir), spark.sparkContext.hadoopConfiguration)
        val itemlist = fileSystem.listFiles(new Path(rawS3Dir), true)
        // val subList = itemlist.slice(1,100000) <- not a list more a iter

        val limit = 10000
        var cnt = 1

        def loopy(iter: RemoteIterator[LocatedFileStatus]) : ListBuffer[String] = {

            var files = new ListBuffer[String]()
            
            while(cnt <  limit) {
                if (iter.hasNext) {
                    val uri = iter.next.getPath.toUri.toString
                    files += uri
                    cnt += 1
                } else {
                    cnt += 1
                }
            }

            return files 
        }

        val fileList = loopy(itemlist).toList
        val df = spark.read.parquet(fileList: _*)

        // lets add in a timestamp column and partition with that
        val df2 = df
            .withColumn("RequestTimestamp", to_timestamp($"RequestDateTime", "dd/MMM/yyyy:HH:mm:ss Z"))
            .withColumn("RequestYear", year(col("RequestTimestamp")))
            .withColumn("RequestMonth", month(col("RequestTimestamp")))
            .withColumn("RequestDay", dayofmonth(col("RequestTimestamp")))
            .orderBy(col("RequestTimestamp"))

        //repartition by our date fields
        val repartitionDF = df2
            .repartition(col("RequestYear"), col("RequestMonth"), col("RequestDay"))
        
        // outputdir
        val destinationS3Dir = "s3a://blaws3logsorganised/repartitioned_dir"

        repartitionDF
            .write
            .partitionBy("RequestYear", "RequestMonth", "RequestDay")
            .parquet(destinationS3Dir)

        spark.stop()

    }

}