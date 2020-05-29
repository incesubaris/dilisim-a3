package SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkSql1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);

    /* SparkSession */
    val ss = SparkSession.builder()
      .appName("RETAIL")
      .master("local")
      .getOrCreate()

    /* orders */
    val schema_orders = new StructType() // orderId,orderDate,orderCustomerId,orderStatus
      .add("orderId",IntegerType,false)
      .add("orderDate",DateType,true)
      .add("orderCustomerId",IntegerType,false)
      .add("orderStatus",StringType,false)

    val orders = ss.read.format("csv")
      .option("header", "true")
      .schema(schema_orders)
      .load("./datasets/orders.csv")
    //orders.printSchema()
    //orders.show(false)

    orders.createOrReplaceTempView("orders")

    /* order_items */
    val schema_order_items = new StructType() // orderItemName,orderItemOrderId,orderItemProductId,orderItemQuantity,orderItemSubTotal,orderItemProductPrice
      .add("orderItemName",IntegerType,false)
      .add("orderItemOrderId",IntegerType,false)
      .add("orderItemProductId",IntegerType,false)
      .add("orderItemQuantity",IntegerType,false)
      .add("orderItemSubTotal",DoubleType,false)
      .add("orderItemProductPrice",DoubleType,false)

    val order_items = ss.read.format("csv")
      .option("header", "true")
      .schema(schema_order_items)
      .load("./datasets/order_items.csv")
    //order_items.printSchema()
    //order_items.show(false)

    order_items.createOrReplaceTempView("order_items")

    /* products */
    val schema_products = new StructType() // productId,productCategoryId,productName,productDescription,productPrice,productImage
      .add("productId",IntegerType,false)
      .add("productCategoryId",IntegerType,false)
      .add("productName",StringType,false)
      .add("productDescription",StringType,true)
      .add("productPrice",DoubleType,false)
      .add("productImage",StringType,true)

    val products = ss.read.format("csv")
      .option("header", "true")
      .schema(schema_products)
      .load("./datasets/products.csv")
    //products.printSchema()
    //products.show(false)

    products.createOrReplaceTempView("products")

    /* categories */
    val schema_categories = new StructType() // categoryId,categoryDepartmentId,categoryName
      .add("categoryId",IntegerType,false)
      .add("categoryDepartmentId",IntegerType,false)
      .add("categoryName",StringType,false)

    val categories = ss.read.format("csv")
      .option("header", "true")
      .schema(schema_categories)
      .load("./datasets/categories.csv")
    //categories.printSchema()
    //categories.show(false)

    categories.createOrReplaceTempView("categories")


    val canceled = ss.sql("SELECT * FROM orders,order_items WHERE orders.orderId = order_items.orderItemOrderId AND orders.orderStatus ='CANCELED' ORDER BY order_items.orderItemSubTotal DESC")
    canceled.createOrReplaceTempView("canceled")
    //canceled.show()

    val canceled_products = ss.sql("SELECT orderItemProductId, productCategoryId, productName, orderItemSubTotal FROM canceled,products WHERE canceled.orderItemProductId = products.productId ORDER BY canceled.orderItemSubTotal DESC")
    canceled_products.createOrReplaceTempView("canceled_products")
    //canceled_products.show()

    val top_product = ss.sql("SELECT orderItemProductId, productName, SUM(orderItemSubTotal) FROM canceled_products GROUP BY orderItemProductId, productName ORDER BY SUM(orderItemSubTotal) DESC")
    top_product.show()
    top_product.write.parquet("output/product.parquet")

    val canceled_categories = ss.sql("SELECT * FROM canceled_products,categories WHERE canceled_products.productCategoryId = categories.categoryId ORDER BY canceled_products.orderItemSubTotal DESC")
    canceled_categories.createOrReplaceTempView("canceled_categories")
    //canceled_categories.show()


    val top_categories = ss.sql("SELECT categoryName, SUM(orderItemSubTotal) FROM canceled_categories GROUP BY categoryName ORDER BY SUM(orderItemSubTotal) DESC")
    top_categories.show()
    top_categories.write.parquet("output/categories.parquet")

  }
}
