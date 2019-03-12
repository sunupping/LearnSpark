import scala.collection.mutable.ArrayBuffer

/**
  * Created by jie.sun on 2018/9/26.
  */
class TestExample {

  def trimNotFirst(arry :Array[Int]): Unit ={
    var first = true
    //首先收集需要标记的下标
    val indexs = for (i <- 0 until arry.length if first || arry(i)>0) yield {
      if (arry(i) < 0) first = false;i
    }
    //注意：从数组缓冲中移除元素并不高效，把非负数值拷贝到前端要好的多
    //然后将元素移动到该去的位置，并截断尾端
    for (j <- 0 until indexs.length) arry(j) = arry(indexs(j))
    //数组转换成数组缓冲，数组缓冲可以进行
    arry.toBuffer.trimEnd(arry.length - indexs.length)
    arry

  }

  def nArrayRandom(n:Int): Array[Int] ={
    val arr = new Array[Int](n)
    for (x <- (0 until n)){arr(x) = (new util.Random).nextInt(n)}
    arr
  }

}
object TestExample{

  def main(args: Array[String]): Unit = {
//    val aa = new TestExample()
//    val arr = Array("hello tom","hai arry","hello hello")
//    val count = arr.flatMap(s => s.split(" ")).map(a => (a,1))

//    val scores = new scala.collection.mutable.
    val str =
      """
        |select
        | name,
        | age,
        | school
        |from
        | student
        |where
        | age > 12
        |group by
        | class
        |order by
        | age desc
      """.stripMargin
    println(str)


  }
}
