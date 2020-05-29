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
      .load("C:\\Users\\msi\\Desktop\\orders.csv")
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
      .load("C:\\Users\\msi\\Desktop\\order_items.csv")
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
      .load("C:\\Users\\msi\\Desktop\\products.csv")
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
      .load("C:\\Users\\msi\\Desktop\\categories.csv")
    //categories.printSchema()
    //categories.show(false)

    categories.createOrReplaceTempView("categories")


    val step1 = ss.sql("SELECT * FROM orders WHERE orderStatus ='CANCELED'")
    step1.show()
    //println("step1.count()")
    //println(step1.count())
    //step1.printSchema()

    /*val abc = order_items.join(step1,order_items("orderItemOrderId") ===  step1("orderId"),"inner")
      .show(false)*/


    ss.sql("SELECT * FROM orders,order_items WHERE orders.orderId = order_items.orderItemOrderId AND orders.orderStatus ='CANCELED'").show(false)




    //val abc2 = abc.sortWithinPartitions("order_items.orderItemSubTotal")

    //val abc2 = ss.sql("SELECT form")





    //val step2 = ss.sql("SHOW DATABASES")
    //step2.show()

    /*val step3 = ss.sql("CREATE DATABASE dilisim")
    step3.show()*/

    //val step3 = ss.sql("SHOW TABLES")
    //step3.show()



    //val step4 = ss.sql("ALTER ")
    //step4.show()

    //df.createOrReplaceTempView("test")

    /*ss.sql(
      sqlText =
        """
          SELECT FROM orders
          WHERE orderStatus LIKE 'CANCELED'
          """).show(numRows = 100)*/
    //df.show()

  }
}
