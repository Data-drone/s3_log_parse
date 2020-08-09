// WIP to repack the raw files

package data.drone

/* Spark app to compress my parquet data down */
import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{to_timestamp, year, month, dayofmonth}
import org.apache.spark.sql.types._

object RepackRaw {

    def main(args: Array[String]) {

        //jceks file in cluster config not working yet
        // manually setting in spark shell works
        val spark = SparkSession
            .builder()
            .appName("CompressApp")
            .config("spark.executor.cores", "2")
            .config("spark.num.executors", "10")
            .config("spark.executor.memory", "2g")
            .getOrCreate()

        // need to have the slash at the end
        val rawS3Data = "s3a://blaws3logs/"

        val fileSystem = FileSystem.get(URI.create(rawS3Data), spark.sparkContext.hadoopConfiguration)
        val itemlist = fileSystem.listFiles(new Path(rawS3Data), true)
        
        // need to match up to line 43
        
        val file_filter = "s3serveraccesslogging-alpha2-prod2020-06-05*"

        val read_filter = rawS3Data + file_filter

        val s3logSchema = StructType(Array(
            StructField("BucketOwner", StringType, true),
            StructField("Bucket", StringType, true),
            StructField("RequestDateTime", StringType, true),
            StructField("RemoteIP", StringType, true),
            StructField("Requester", StringType, true),
            StructField("RequestID", StringType, true),
            StructField("Operation", StringType, true),
            StructField("Key", StringType, true),
            StructField("RequestURI_operation", StringType, true),
            StructField("RequestURI_key", StringType, true),
            StructField("RequestURI_httpProtoversion", StringType, true),
            StructField("HTTPstatus", StringType, true),
            StructField("ErrorCode", StringType, true),
            StructField("BytesSent", StringType, true),
            StructField("ObjectSize", StringType, true),
            StructField("TotalTime", StringType, true),
            StructField("TurnAroundTime", StringType, true),
            StructField("Referrer", StringType, true),
            StructField("UserAgent", StringType, true),
            StructField("VersionId", StringType, true),
            StructField("HostId", StringType, true),
            StructField("SigV", StringType, true),
            StructField("CipherSuite", StringType, true),
            StructField("AuthType", StringType, true),
            StructField("EndPoint", StringType, true),
            StructField("TLSVersion", StringType, true)
        ))


        // yes this would work but we have too many files...
        // need more RAM
        val df = spark.read.text(read_filter)

        spark.stop()

    }

}