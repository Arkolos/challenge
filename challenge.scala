import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

val join_on = "listings.manufacturer_formatted = LOWER(products.manufacturer) AND LOWER(listings.title) REGEXP products.regexp_searched"

/* 
  initSql load the .txt files, register them as tables in sqlContext, and add the customs columns  
*/
def initSql(products_file:String, listings_file:String) = 
{
  /*
  We add 3 customs colums :
  In Products :
  - regexp_searched : the regexp that need to be found in a listing title
  - product_name_formatted : the product_name we work with (since sometime several product_name can correspond to different products)

  In Listings:
  - manufacturer_formatted : the manufacturer we will try to match with the product manufacturer
  */

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

  val productname_format: (String => String) = (entry: String) =>
  {
    //sometime, diferent product_name seems to correspond to the same product. We add a column to handle this case
    if (entry == "Canon_EOS_Rebel_T2i" || entry == "Canon_Kiss_X4") "Canon_EOS_550D"
    else if (entry == "Canon_EOS_Rebel_T1i") "Canon_EOS_500D"
    else if (entry == "Samsung-SL202") "Samsung_SL202"
    else entry
  }

  val listings_manufacturer_format: (String => String) = (entry: String) =>
  {
    val sp = entry.split(" ")
    if (sp.length >= 2 && sp(1).toLowerCase() == "kodak")  "kodak"
    else if (sp(0).toLowerCase() == "fuji")  "fujifilm"
    else if (sp(0).toLowerCase().startsWith("hewlett packard")) "hp"
    else sp(0).toLowerCase()
    // Other transformation could be opered, like "GE" => "General Electric" but we don't care since there is no products from this manufacturer
  }

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

def countNotMatchedListings():Long = 
{
  val sqlRequest_notMatched = "SELECT COUNT(*) AS nb FROM products RIGHT JOIN listings ON "+ join_on +" WHERE product_name IS NULL"
  sqlContext.sql(sqlRequest_notMatched).first()(0).asInstanceOf[Long]
}

def countMatchedTwiceListings():Long =
{
  val sqlRequest_dual = "SELECT count(DISTINCT products.product_name_formatted) AS nb, listings.title, listings.manufacturer FROM products  join listings on "+ join_on +" group by listings.title,listings.manufacturer having nb > 1"
  sqlContext.sql(sqlRequest_dual).count
}

def getResult():org.apache.spark.rdd.RDD[(String, Iterable[Listing])] =
{
  val request = "SELECT product_name,listings.* FROM products join listings on "+ join_on

  val sqlResult = sqlContext.sql(request)

  sqlResult.map(p => (p(0).asInstanceOf[String] , (Listing(p(1).asInstanceOf[String], p(2).asInstanceOf[String], p(3).asInstanceOf[String], p(4).asInstanceOf[String])))).groupByKey()
}

def encodeJson(src: AnyRef): JValue = {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.JsonDSL.WithDouble._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)

    Extraction.decompose(src)
}

case class Listing(currency:String,manufacturer:String,price:String,title:String)

initSql("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/products.txt", "/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/listings.txt" )
println ("no matched "+ countNotMatchedListings())
println ("matched twice "+ countMatchedTwiceListings())

val results_grouped = getResult()
val results_json =  results_grouped.map(p => compact(render(encodeJson( ("product_name"-> p._1 ) ~ ("listings" -> encodeJson(p._2))  ))))
results_json.saveAsTextFile("/Users/Arkolos/Dropbox2/Dropbox/Prog/sparc/challenge_sortable/result-json-17")

