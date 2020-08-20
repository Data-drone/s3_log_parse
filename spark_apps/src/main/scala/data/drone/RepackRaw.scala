// WIP to repack the raw files

package data.drone

/* Spark app to compress my parquet data down */
import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{to_timestamp, year, month, dayofmonth, col, hour, to_date, regexp_extract, split}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat


class RepackRaw {

    def main(args: Array[String]) {

        // SaveMode Append is really slow with big data.
        // generally we are reading folder by folder there will be no duplicates across dates
        // so we can overwrite 
        // without the dynamic partition overwrite overwrite will wipe the folder
        val spark = SparkSession
            .builder()
            .appName("RepackApp")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        
        // need to have the slash at the end
        // val rawS3Data = "s3a://blaws3logs/"
        // val rawTest = "s3a://blaws3logsorganised/datesort/20-06-05/s3serveraccesslogging-alpha2-prod2020-06-05-09*" 
        // val rawByDate = "s3a://blaws3logsorganised/datesort/20-06-0*"

        // val df = spark.read.text(rawPath+"/*")

        // add code changes to step through days instead of processing ta once?
        process(args(0), args(1), args(2), args(3), spark)
        
        spark.stop()

    }

    def process(readPath: String, writePath: String, 
                    startDate: String, endDate: String, spark: SparkSession): Unit = {

        // date format = yyyy-mm-dd
        val startDateFormat = DateTime.parse(startDate)
        val endDateFormat = DateTime.parse(endDate)

        // this is the folder structure that we have
        val formatter  = DateTimeFormat.forPattern("yy-MM-dd")
        
        if (startDateFormat == endDateFormat) {
            val reformatDate = startDateFormat.toString(formatter)
            val readDir = readPath + reformatDate

            transform(readDir, writePath, spark)
        } else {

            val daysCount = Days.daysBetween(startDateFormat, endDateFormat).getDays()
            for (i <- 0 until daysCount) {
                val x = startDateFormat.plusDays(i)
                val reformatDate = x.toString(formatter)
                val readDir = readPath + reformatDate
                transform(readDir, writePath, spark)
            }

        }

    }

    

}