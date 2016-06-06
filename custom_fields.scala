/*
Those 3 functions are used in initSql to calculate the customs fields
*/

def regexp_searched_create: ((String, String) => String) = (model: String, family:String) =>  
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

def productname_format: (String => String) = (entry: String) =>
{
  //sometimes, different product_name seems to correspond to the same product. We add a column to handle this case
  if (entry == "Canon_EOS_Rebel_T2i" || entry == "Canon_Kiss_X4") "Canon_EOS_550D"
  else if (entry == "Canon_EOS_Rebel_T1i") "Canon_EOS_500D"
  else if (entry == "Samsung-SL202") "Samsung_SL202"
  else entry
}

def listings_manufacturer_format: (String => String) = (entry: String) =>
{
  val sp = entry.split(" ")
  if (sp.length >= 2 && sp(1).toLowerCase() == "kodak")  "kodak"
  else if (sp(0).toLowerCase() == "fuji")  "fujifilm"
  else if (sp(0).toLowerCase().startsWith("hewlett packard")) "hp"
  else sp(0).toLowerCase()
  // Other transformation could be opered, like "GE" => "General Electric" but we don't care since there is no products from this manufacturer
}