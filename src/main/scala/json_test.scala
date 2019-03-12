
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/**
  * Created by jie.sun on 2018/10/25.
  */
/**
  * Created by jie.sun on 2018/10/25.
  */
object json_test {

  case class MsgBody(shopId:Int,id:Long,systemNum:Int,soId:String,isOpenMq:Boolean,warehouseCode:String,interfaceShopCode:String,isOrderRoute:Boolean,code:String,
                     retryCount:Int,rasId:String,isOpenQiMen:Boolean,platformOrderCode:String,platformOrderCodeN:String,syncFlag:String)
  case class TP_rmq2bi_wms(msgType:String,sendTime:String,msgBody:List[MsgBody])
  def main(args: Array[String]): Unit = {
    //从json中抽取数据
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
//    val testjson = """{"name":"joe","age":15,"luckNumbers":[1,2,3,4,5]}"""
    val json =
      """{msgType:"MQ_toms2toms_so_wms_order_pro",sendTime:"2018-10-25 09:55:21",msgBody:
        |"[{"shopId":176701,"id":696342536,"systemNum":6,"soId":null,"isOpenMq":false,"warehouseCode":"TEST-SKE-LF",
        |"interfaceShopCode":"TBITSTORE-SKX","isOrderRoute":true,"code":null,"retryCount":0,
        |"rasId":null,"isOpenQiMen":true,"platformOrderCode":"40000709931629",
        |"platformOrderCodeN":"40000709931629","syncFlag":null}]"}""".stripMargin
//    val p = parse(json).extract[TP_rmq2bi_wms]
//    val a = p.msgBody(0).shopId
//    val q = parse(p.msgBody(0)).extract[MsgBody]
    println(json.substring(84,json.length-3))
//    println(a)
//    println(p.msgType)
  }
}
