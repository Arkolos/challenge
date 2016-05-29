import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

def encodeJson(src: AnyRef): JValue = {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.JsonDSL.WithDouble._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)

    Extraction.decompose(src)
}

case class Listing(currency:String,manufacturer:String,price:String,title:String)

/*
class Listing(currency_p:String,manufacturer_p:String,price_p:Float,title_p:String) {
  val currency:String = currency_p
  val manufacturer:String = manufacturer_p
  val price:Float = price_p
  val title:String = title_p

  def toJSON():String = "{\"title\":\"" + currency + "\",\"manufacturer\":\"" + manufacturer + "\",\"currency\":\"" + price + "\",\"price\":\"" + title + "\"}"
  //override def toString(): String = this.toJSON()
}*/

val formate_model: (String => String) = (entry: String) =>  entry.replaceAll("[-_ ]", "[-_ ]?") //eg : "QV-5000SX" ->  "QV[-_ ]?5000SX"
val sqlfunc = udf(formate_model)

//def listingtoJSON(l:Listing):String = "{\"title\":\"" + l.currency + "\",\"manufacturer\":\"" + l.manufacturer + "\",\"currency\":\"" + l.price + "\",\"price\":\"" + l.title + "\"}"

val products_fromfile = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/products.txt")
val products =  products_fromfile.withColumn("model_formated", sqlfunc(col("model")))
products.registerTempTable("products")

val sqlfunc = udf(formate_model)

val listings = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/listings.txt")
listings.registerTempTable("listings")

val result_sql = sqlContext.sql("SELECT product_name,listings.* FROM products  join listings on listings.manufacturer = products.manufacturer AND listings.title REGEXP products.model_formated")

val results_grouped = result_sql.map(p => (p(0).asInstanceOf[String] , (Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String])))).groupByKey()

//val results_json =  results_grouped.map(p => compact(render(encodeJson( (("product_name", p._1 ), ("listings", p._2))  ))))

val results_json =  results_grouped.map(p => compact(render(encodeJson( ("product_name"-> p._1 ) ~ ("listings" -> encodeJson(p._2))  ))))
results_json.saveAsTextFile("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/result-json-withoutspaces")
/*
val results_grouped = result_sql.map(p => (p(0).asInstanceOf[String] , listingtoJSON(Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String])))).groupByKey()


val results_json =results_grouped.map(p => (
    "{\"product_name\": \"" + p._1 + "\", \"listings\": [" + p._2.mkString(", ") + "]}"))



*/





/*

//todo : try with json lib

results_grouped.map(p => (p._1, p._2.map(listingtoJSON) ))


result_sql.map(p => (p(0).asInstanceOf[String] , Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String]))).groupByKey().foreach(println)
*/

//todo : utiliser fucntion join
//todo : meilleur matcheur

//result_sql.map(p => (p(0).asInstanceOf[String] , (p(1), p(2), p(3), p(4)))).groupByKey().foreach(println)