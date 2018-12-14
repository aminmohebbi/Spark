import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException,DataFrame,Dataset,SparkSession}

object sapn {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    implicit val spark:SparkSession = SparkSession
      .builder()
      .appName("Test")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    case class RepoStarring (
                            user_id:Int,
                            repo_id :Int,
                            starred_at : java.sql.Timestamp,
                            starring: Double
                            )
    val dataDir ="."
    val today = "20170820"

    val savePath = $"dataDir"/spark-data/$today/repostarringDF.parquet
    val df = try {
      spark.read.parquet(savePath)
    } catch {
      case e : AnalysisException => {
        if (e.getMessage().contains("Path does not exist"))
          print("path does not exist please check that")
        else {
          throw e
        }
      }
    }
  }
}
