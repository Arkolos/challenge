Sortable coding challenge
=========================

Subject hear : http://sortable.com/challenge/

"
The goal of the project is to match product listings from a 3rd party retailer, e.g. “Nikon D90 12.3MP Digital SLR Camera (Body Only)” against a set of known products, e.g. “Nikon D90”.
"

How to run
----------

You need Spark with Scala and the package spark-csv

The program then create the file XXX and display how many lines 

This project had been tested with Spark  1.6.1, Scala 2.10.5, and spark-csv 1.3.0:
./bin/spark-shell --packages com.databricks:spark-csv_2.11:1.3.0 XXX

How does it works :
-------------------

The project first create for each product a REGEXP that need to be found in the listing title if the listing is about this product. This REGEXP is mostly based on the "model" field of the product. For a model "A3000 IS" , the regexp will be "[^-_a-z0-9]a[-_ ]?3000[-_ ]?is[^-_a-z0-9]".

Then, a jointure is made between the "products" and "listings" table using this calculated "regexp_searched" field and the "manufacturer" one. The sql query is :

SELECT product_name,listings.* FROM products
join listings on listings.manufacturer_formated = LOWER(products.manufacturer) AND LOWER(listings.title) REGEXP products.regexp_searched

Then, the result are grouped according to the "product_name". Some different product_name seems correspond to the same product (see the "Note" part bellow) so they are also regrouped.


Results :
---------

Listings not matched with any product : XXXX
Listing matched with more than 1 product : XXXX

(total listings : 20196, total products : 743)



Note :
------

Some products seems to be the same :
Canon_EOS_550D , Canon_EOS_Rebel_T2i and Canon_Kiss_X4 ( http://www.dpreview.com/products/canon/slrs/canon_eos550d )
-> we choose Canon_EOS_550D

Canon_EOS_500D,Canon_EOS_Rebel_T1i and ( http://www.dpreview.com/reviews/canoneos500d )
-> we choose Canon_EOS_500D

Samsung-SL202 and Samsung_SL202
-> we choose Samsung_SL202


Idea of improvements
====================

We could replace the sql requests by a lower-level treatment. It could :
- improve time performances by controling better what is really done
- allow more flexibility to add new features (for now it is not possible to set priority for products or to do treatment not supported by sql)

