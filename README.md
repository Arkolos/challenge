Sortable coding challenge
=========================

Subject hear : http://sortable.com/challenge/


> The goal of the project is to match product listings from a 3rd party retailer, e.g. “Nikon D90 12.3MP Digital SLR Camera (Body Only)” against a set of known products, e.g. “Nikon D90”.


How to run
----------

You need Spark with Scala and the package spark-csv (tested with Spark  1.6.1, Scala 2.10.5, and spark-csv 1.3.0) 

To run the solution :

- clone this repository

- copy the products.txt and listings.txt files from http://sortable.com/challenge/ in the repository directory, next to the "challenge.scala" and "custom_fields.scala" files

- run :   
```
/path/to/your/spark-shell  --packages com.databricks:spark-csv_2.11:1.3.0 -i custom_fields.scala -i challenge.scala
```

There should have no errors. A "result" directory is created in you current directory. When it's finished, you should get the Scala prompt again.

- Exit the Spark shell (type the "exit" command) and run :   
```
cat result/part-* > result.txt
```

The excepted result is in "result.txt" !


How does it works :
-------------------

The solution first creates for each product a REGEXP, mostly based on the "model" field of the product. For example, for a model "A3000 IS" , the regexp will be "[^-_a-z0-9]a[-_ ]?3000[-_ ]?is[^-_a-z0-9]". This regexp  will be searched in the listings title.

Some different "product_name" correspond to the same product (see the "Note" part bellow) so a custom product_name column is created.

To match, a jointure is made between the "products" and "listings" table using the previously calculated "regexp_searched" field and the "manufacturer" one. The sql query is :

```sql
SELECT product_name_formatted,listings.* FROM products
join listings on listings.manufacturer_formated = LOWER(products.manufacturer) AND LOWER(listings.title) REGEXP products.regexp_searched
```

Then, the result are grouped according to the "product_name_formatted" column and displayed in Json.


Results :
---------

Listings not matched with any product : 11394   
It's normal that a big number of listings don't have a product matching, since not every existing model of camera is listed in products

Listing matched with more than 1 product : 15   
Mostly listings that are about several products, eg. battery and kit for several cameras. 

(total listings : 20196, total products : 743)



Note :
------

Some products seems to be the same :  
- Canon_EOS_550D , Canon_EOS_Rebel_T2i and Canon_Kiss_X4 ( http://www.dpreview.com/products/canon/slrs/canon_eos550d )   
-> we choose Canon_EOS_550D

- Canon_EOS_500D and Canon_EOS_Rebel_T1i  ( http://www.dpreview.com/reviews/canoneos500d )   
-> we choose Canon_EOS_500D

- Samsung-SL202 and Samsung_SL202   
-> we choose Samsung_SL202


Idea of improvements
====================

- We could remove from the result all the listings that match with more than one product 

- We could replace the sql requests by a lower-level treatment. It could :

    - improve time performances by controling better what is really done

    - allow more flexibility to add new features (eg : for now it is not possible to set priority for products or to realize treatments not supported by sql)

