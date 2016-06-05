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
val join_on = "listings.manufacturer_formatted = LOWER(products.manufacturer) AND LOWER(listings.title) REGEXP products.regexp_searched"

def countNotMatchedListings():Long = 
{
  val sqlRequest_notMatched = "SELECT product_name,listings.* FROM products right join listings on "+ join_on +" WHERE product_name IS NULL"
  sqlContext.sql(sqlRequest_notMatched).count
}

def countMatchedTwiceListings():Long =
{
  val sqlRequest_dual = "SELECT count(DISTINCT products.product_name_formatted) AS nb, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" group by listings.title,listings.manufacturer having nb > 1"
  sqlContext.sql(sqlRequest_dual).count
}

case class Listing(currency:String,manufacturer:String,price:String,title:String)

def initSql() = 
{
  val regexp_searched_create: ((String, String) => String) = (model: String, family:String) =>  
  {
    /* The regexp returned try to match with LOWER(title) */
    val family_lower = if (family != null) family.toLowerCase() else null

    val regexp_basis_lower = 
      if (family_lower == "elph" || family_lower == "ixus" || family_lower == "stylus" || family_lower == "d-lux" || family_lower == "digilux") family_lower + " " + model.toLowerCase() 
      else if (family_lower == "digital ixus") "ixus " + model.toLowerCase()
      else model.toLowerCase()

    val regexp_basis_separete = regexp_basis_lower.replaceAll("([A-z])([0-9])", "$1-$2") // eg : "PLOP23" -> "PLOP-23"
                                            .replaceAll("([0-9])([A-z])", "$1-$2")

    val regexp = regexp_basis_separete.replaceAll("([\\^\\$\\.\\[\\{\\\\\\?\\|\\+\\*\\(\\)])", "\\\\$1") // escape if there is a regexp special char in the product model
                                      .replaceAll("[-_ ]", "[-_ ]?")//eg : "QV-5000SX" ->  "QV[-_ ]?5000SX"

    val bound = "[^-_a-z0-9]"

    //When the family is cybershot, a "dsc" prefix is sometime added. And a suffix one-letter suffix too, to indicate the camera color
    val before = if ((family_lower == "cybershot" || family_lower == "cyber-shot") && !regexp_basis_lower.startsWith("dsc")) "(dsc[-_ ]?)?" else ""
    val after = if ((family_lower == "cybershot" || family_lower == "cyber-shot") ) "[a-uw-z]?" else ""
    
    val after_bound =  if (model == "WG-1") "[^g]" // so that the models "WG-1" and "WG-1 GPS" are not confused
                       else if (model == "Digilux") "[^4]" // so that the models "Digilux 4.3" and "Digilux" are not confused
                       else "" 

    bound + before + regexp + after + bound + after_bound
  }

  val productname_formate: (String => String) = (entry: String) =>
  {
    //somethime, different product_name seems to correspond to the same product. We add a column to handle this case
    if (entry == "Canon_EOS_Rebel_T2i" || entry == "Canon_Kiss_X4") "Canon_EOS_550D"
    else if (entry == "Canon_EOS_Rebel_T1i") "Canon_EOS_500D"
    else if (entry == "Samsung-SL202") "Samsung_SL202"
    else entry
  }

  val listings_manufacturer_formate: (String => String) = (entry: String) =>
  {
    val sp = entry.split(" ")
    if (sp.length >= 2 && sp(1).toLowerCase() == "kodak")  "kodak"
    else if (sp(0).toLowerCase() == "fuji")  "fujifilm"
    else if (sp(0).toLowerCase().startsWith("hewlett packard")) "hp"
    else sp(0).toLowerCase()
    // Other transformation could be opered, like "GE" => "General Electric" but we don't care since there is no products from this manufacturer
  }

  val sqlfunc_regexp = udf(regexp_searched_create)
  val sqlfunc_manufacturer = udf(listings_manufacturer_formate)
  val sqlfunc_productname = udf(productname_formate)

  val products_fromfile = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/products.txt")
  val products =  products_fromfile.withColumn("regexp_searched", sqlfunc_regexp(col("model"), col("family")))
                                    .withColumn("product_name_formatted", sqlfunc_productname(col("product_name")))
  products.registerTempTable("products")

  val listings_fromfile = sqlContext.read.json("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/listings.txt")
  val listings = listings_fromfile.withColumn("manufacturer_formatted", sqlfunc_manufacturer(col("manufacturer")))
  listings.registerTempTable("listings")
}

def getResultSQL():org.apache.spark.sql.DataFrame =
{
  
  val request = "SELECT product_name,listings.* FROM products  join listings on "+ join_on

  sqlContext.sql(request)

}
initSql()
println ("no matched "+ countNotMatchedListings())
println ("matched twice "+ countMatchedTwiceListings())

  val result_sql = getResultSQL()

/*
val sqlRequest_dual = "SELECT count(DISTINCT products.product_name, products.manufacturer, products.model, products.family) AS nb, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" group by listings.title,listings.manufacturer having nb > 1"
  val result = sqlContext.sql(sqlRequest_dual)
  result.write.format("com.databricks.spark.csv").option("header", "true").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/matchedtwice5.csv")
*/
//maybe products.manufacturer .+ useless

val sqlRequest_dual = "SELECT products.product_name,products.product_name_formatted, products.regexp_searched , products.manufacturer, products.model, products.family, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" "
val sql = sqlContext.sql(sqlRequest_dual)
sql.write.format("com.databricks.spark.csv").option("header", "true").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/matchjoin17.csv")

  

  val sqlRequest_notMatched = "SELECT product_name,listings.* FROM products right join listings on "+ join_on +" WHERE product_name IS NULL"
  
  val result_sql_nofound = sqlContext.sql(sqlRequest_notMatched)

  result_sql_nofound.write.format("com.databricks.spark.csv").option("header", "true").save("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/nomatched17.csv")

  
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