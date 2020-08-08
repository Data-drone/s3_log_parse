package data.drone

/* Spark app to compress my parquet data down */
import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import scala.collection.mutable.ListBuffer

object CompressTable {

    def main(args: Array[String]) {

        /*start session*/
        val spark = SparkSession
            .builder()
            .appName("CompressApp")
            .config("spark.executor.cores", "2")
            .config("spark.num.executors", "10")
            .config("spark.executor.memory", "2g")
            .getOrCreate()

        /* Get a file list to run quick exps */
        val rawS3Dir = "s3a://blaws3logsorganised/RawParquet_test2"
        val fileSystem = FileSystem.get(URI.create(rawS3Dir), new Configuration())
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

        val destinationS3Dir = "s3a://blaws3logsorganised/repartitioned_dir"
    
        val df = spark.read.parquet(fileList: _*)


        val repartitionDF = df.repartition(30)
        
        repartitionDF.write.parquet(destinationS3Dir)

        spark.stop()

    }

}