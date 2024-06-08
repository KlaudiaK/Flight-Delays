package put.poznan.pl.klaudiak
package consumers

import java.sql.{Connection, DriverManager}

object JdbcConsumer extends App {
  if (args.length != 3)
    throw new NoSuchElementException

  var connection: Connection = _
  try {
    Class.forName("org.postgresql.Driver")
    connection = DriverManager.getConnection(args(0), args(1), args(2))
    System.out.println("Connected to database")

    val statement = connection.createStatement
    while(true) {
      val result = statement.executeQuery("SELECT * FROM delays ORDER BY day DESC LIMIT 50")
      print("\u001b[2J")
      println("================ NEW DATA ================")
      while(result.next) {
        val day = result.getDate("day")
        val state = result.getString("state")
        val departure_count = result.getString("departure_count")
        val total_departure_delay = result.getString("total_departure_delay")
        val arrival_count = result.getString("arrival_count")
        val total_arrival_delay = result.getString("total_arrival_delay")

        println(s"$day \t $state \t $total_departure_delay \t $departure_count \t $arrival_count \t $total_arrival_delay")
      }
      Thread.sleep(10000)
    }
  } catch {
    case e: Exception => e.printStackTrace()
  }
  connection.close()
}