package com.rockthejvm

import org.apache.spark.sql.api.java.UDF1
import scala.annotation.tailrec

// returns the number of occurrences of the word "rockthejvm" in a value
/*
    Steps
        - put this in an IDE
        - compile this code to a JAR
        - copy the JAR to your PySpark installation
        - use the JAR location in spark.conf
*/
class Occurrences extends UDF1[String, Int] {
  val token = "rockthejvm"

  override def call(value: String): Int = {
    @tailrec
    def loop(remainder: String, acc: Int): Int = {
      val index = remainder.indexOf(token)
      if (index == -1) acc
      else loop(remainder.substring(index + token.length), acc + 1)
    }

    loop(value, 0)
  }
}

object Test {
  def main(args: Array[String]): Unit = {

    val occ = new Occurrences
    println(occ.call("This is rockthejvm, go to rockthejvm.com for the ultimate courses on Apache Spark."))
  }
}
