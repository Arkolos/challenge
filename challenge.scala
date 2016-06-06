import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class Listing(currency:String,manufacturer:String,price:String,title:String)

val join_on = "listings.manufacturer_formatted = LOWER(products.manufacturer) AND LOWER(listings.title) REGEXP products.regexp_searched"

:load custom_fields.scala //functions used to calculate the customs fields
/* 
  initSql load the .txt files, register them as tables in sqlContext, and add the customs columns  
*/
def initSql(products_file:String, listings_file:String) = 
{
  /*
  We add 3 customs fields :
  In Products :
  - regexp_searched : the regexp that need to be found in a listing title
  - product_name_formatted : the product_name we work with (since sometime several product_name can correspond to different products)

  In Listings:
  - manufacturer_formatted : the manufacturer we will try to match with the product manufacturer
  */

  val sqlfunc_regexp = udf(regexp_searched_create)
  val sqlfunc_manufacturer = udf(listings_manufacturer_format)
  val sqlfunc_productname = udf(productname_format)

  val products_fromfile = sqlContext.read.json(products_file)
  val products =  products_fromfile.withColumn("regexp_searched", sqlfunc_regexp(col("model"), col("family")))
                                    .withColumn("product_name_formatted", sqlfunc_productname(col("product_name")))
  products.registerTempTable("products")

  val listings_fromfile = sqlContext.read.json(listings_file)
  val listings = listings_fromfile.withColumn("manufacturer_formatted", sqlfunc_manufacturer(col("manufacturer")))
  listings.registerTempTable("listings")
}

/*
getResult perform the request and group result by product
*/
def getResult():org.apache.spark.rdd.RDD[(String, Iterable[Listing])] =
{
  val request = "SELECT product_name_formatted,listings.* FROM products JOIN listings on "+ join_on

  val sqlResult = sqlContext.sql(request)

  sqlResult.map(p => (p(0).asInstanceOf[String] , (Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String])))).groupByKey()
}

/* 
countNotMatchedListings and countMatchedTwiceListings are used to get the matching stats
 */
def countNotMatchedListings():Long = 
{
  val sqlRequest_notMatched = "SELECT COUNT(*) AS nb FROM products RIGHT JOIN listings ON "+ join_on +" WHERE product_name IS NULL"
  sqlContext.sql(sqlRequest_notMatched).first()(0).asInstanceOf[Long]
}
def countMatchedTwiceListings():Long =
{
  val sqlRequest_dual = "SELECT count(DISTINCT products.product_name_formatted) AS nb, listings.title, listings.manufacturer FROM products JOIN listings on "+ join_on +" GROUP BY listings.title,listings.manufacturer HAVING nb > 1"
  sqlContext.sql(sqlRequest_dual).count
}

def encodeJson(src: AnyRef): JValue = {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.JsonDSL.WithDouble._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)

    Extraction.decompose(src)
}

initSql("products.txt", "listings.txt" )

val results_grouped = getResult()
val results_json =  results_grouped.map(p => compact(render(encodeJson( ("product_name"-> p._1 ) ~ ("listings" -> encodeJson(p._2))  ))))
results_json.saveAsTextFile("result")

/* Uncomment to display the stats */
// println ("no matched "+ countNotMatchedListings())
// println ("matched twice "+ countMatchedTwiceListings())