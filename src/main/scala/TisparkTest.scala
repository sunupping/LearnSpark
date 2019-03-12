import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by jie.sun on 2018/9/28.
  */
object TisparkTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      println("Usage:spark-submit *.jar  <schema>  <sql>   <ip>   <port>  <username>  <password>  <schema.tablename>  [mode]")
      System.exit(0)
    }

    //val schema = args(0)
    val sqlText = args(1)
    val ip = args(2)
    val port = args(3)
    val username = args(4)
    val password = args(5)
    val tablename = args(6)
    var md = ""
    if (args.length == 8){md = args(7)}
    val mod = md match {
        case "append" => SaveMode.Append
        case "Append" => SaveMode.Append
        case "overwrite" => SaveMode.Overwrite
        case "Overwrite" => SaveMode.Overwrite
        case _ => SaveMode.ErrorIfExists
    }

    val spark = SparkSession.builder().appName("tispark by BI").getOrCreate()
    import org.apache.spark.sql.TiContext
    val ti = new TiContext(spark)
    //参数1 需要加载的数据库
    ti.tidbMapDatabase(args(0))
    import spark.sql
    //查询数据并保存数据 参数2 需要执行的sql语句
    //val kvDf = sql(args(1))
    //保存数据 参数& 保存模式
    sql(sqlText).write.format("jdbc").option("url", s"jdbc:mysql://${ip}:${port}").option("dbtable", s"${tablename}").option("user", s"${username}").option("password", s"${password}").mode(mod).save()

  }
}
