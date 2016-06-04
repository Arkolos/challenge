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


//val join_on = "listings.manufacturer LIKE CONCAT(LOWER(products.manufacturer), '%') AND listings.title REGEXP products.model_formated "
val join_on = "listings.manufacturer_formated = products.manufacturer AND listings.title REGEXP products.model_formated"

def countNotMatchedListings():Long = 
{
  val sqlRequest = "SELECT product_name,listings.* FROM products right join listings on "+ join_on +" WHERE product_name IS NULL"
  sqlContext.sql(sqlRequest).count
}

def countMatchedTwiceListings():Long =
{
  val sqlRequest_dual = "SELECT count(DISTINCT products.product_name, products.manufacturer, products.model, products.family) AS nb, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" group by listings.title,listings.manufacturer having nb > 1"
  sqlContext.sql(sqlRequest_dual).count
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

def getResultSQL():org.apache.spark.sql.DataFrame =
{
  val formate_model: (String => String) = (entry: String) =>  " " + entry.replaceAll("[-_ ]", "[-_ ]?") + " " //eg : "QV-5000SX" ->  "QV[-_ ]?5000SX"
  val formate_manufacturer_listings: (String => String) = _.split(" ")(0)

  val sqlfunc_model = udf(formate_model)
  val sqlfunc_manufacturer = udf(formate_manufacturer_listings)

  val products_fromfile = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/products.txt")
  val products =  products_fromfile.withColumn("model_formated", sqlfunc_model(col("model")))
  products.registerTempTable("products")

  val listings_fromfile = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/listings.txt")
  val listings = listings_fromfile.withColumn("manufacturer_formated", sqlfunc_manufacturer(col("manufacturer")))
  listings.registerTempTable("listings")

  val request = "SELECT product_name,listings.* FROM products  join listings on "+ join_on

  sqlContext.sql(request)

}
  val result_sql = getResultSQL()

println ("no matched "+ countNotMatchedListings())
println ("matched twice "+ countMatchedTwiceListings())


//maybe products.manufacturer .+ useless
//val sqlRequest_dual = "SELECT products.product_name, products.manufacturer, products.model, products.family, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" "
 
//sqlContext.sql(sqlRequest_dual).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/matchtwice.csv")

  /*
  val sqlRequest = "SELECT product_name,listings.* FROM products right join listings on "+ join_on +" WHERE product_name IS NULL"
  
  val result_sql_nofound = sqlContext.sql(sqlRequest)

  result_sql_nofound.write.format("com.databricks.spark.csv").option("header", "true").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/nomatched.csv")

  */
  //result_sql.write.format("com.databricks.spark.csv").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/nomatched.csv")


  val results_grouped = result_sql.map(p => (p(0).asInstanceOf[String] , (Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String])))).groupByKey()

//val results_json =  results_grouped.map(p => compact(render(encodeJson( (("product_name", p._1 ), ("listings", p._2))  ))))

val results_json =  results_grouped.map(p => compact(render(encodeJson( ("product_name"-> p._1 ) ~ ("listings" -> encodeJson(p._2))  ))))
results_json.saveAsTextFile("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/result-json-2")
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