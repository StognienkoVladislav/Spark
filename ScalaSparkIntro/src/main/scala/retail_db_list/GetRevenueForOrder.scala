package retail_db_list

object GetRevenueForOrder {

  def main(args: Array[String]): Unit = {

    val orderItemsPath = args(0)
    val orderId = args(1).toInt

    println("Order Items")
    val orderItems = scala.io.Source.fromFile(orderItemsPath).getLines().toList
    orderItems.take(10).foreach(println)

    println("Items Filtered")
    val orderItemsFiltered = orderItems.filter(
      e => e.split(",")(1).toInt == orderId
    )
    orderItemsFiltered.foreach(println)

    println("Subtotals")
    val orderItemsSubtotals = orderItemsFiltered.
      map(e => e.split(",")(4).toFloat)
    orderItemsSubtotals.foreach(println)

    val orderRevenue = orderItemsSubtotals.reduce((curr, next) => curr + next)
    println("Order revenue for order id " + orderId + " is " + orderRevenue)

  }

}
