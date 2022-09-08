
/*

- move lipsum.txt to Spark Essentials
- remove blank lines
- create new class in the DataFrames chapter
*/


package part2dataframes

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions

object UDAFs {

  val spark = SparkSession.builder()
    .appName("UDAF Example")
    .config("spark.master", "local")
    .getOrCreate()

  case class Movie(title: String, genre: String, rating: Double)
  case class TitleList(titles: List[String])

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val moviesDF = spark.read
      .json("src/main/resources/data/movies.json")
      .selectExpr("Title as title", "Major_Genre as genre", "IMDB_Rating as rating")
      .as[Movie]

    // combine the titles of all movies, by genre, into a single string denoting a list
    object TitleCombiner extends Aggregator[Movie, TitleList, String] {
      // initial value
      def zero: TitleList = TitleList(List())
      // Combine two values to produce a new value. For performance, the function may modify `buffer`
      // and return it instead of constructing a new object
      def reduce(buffer: TitleList, movie: Movie): TitleList =
        buffer.copy(titles = movie.title :: buffer.titles)

      // Merge two intermediate values
      def merge(b1: TitleList, b2: TitleList): TitleList =
        TitleList(b1.titles ++ b2.titles)

      // Transform the output of the reduction
      def finish(reduction: TitleList): String = reduction.titles.mkString("[",",","]")
      // Specifies the Encoder for the intermediate value type
      def bufferEncoder: Encoder[TitleList] = Encoders.product
      // Specifies the Encoder for the final output value type
      def outputEncoder: Encoder[String] = Encoders.STRING
    }

    spark.udf.register("combineTitles", functions.udaf(TitleCombiner))

    moviesDF.createOrReplaceTempView("movies")
    val titlesCombinedDF = spark.sql("select combineTitles(*) from movies group by genre")
    titlesCombinedDF.show()

    // untyped version: the types don't use the entire type of the row of the DF, OR use the entire row as a "row", not as a Scala case class
    object UntypedTitleCombiner extends Aggregator[String, TitleList, String] {
      override def zero = TitleList(List())
      override def reduce(buffer: TitleList, title: String) =
        buffer.copy(titles = title :: buffer.titles)
      override def merge(b1: TitleList, b2: TitleList) =
        TitleList(b1.titles ++ b2.titles)
      override def finish(reduction: TitleList) =
        reduction.titles.mkString("[",",","]")
      def bufferEncoder: Encoder[TitleList] = Encoders.product
      def outputEncoder: Encoder[String] = Encoders.STRING
    }

    spark.udf.register("combineTitlesUntyped", functions.udaf(UntypedTitleCombiner))

    moviesDF.createOrReplaceTempView("movies")
    val titlesCombinedUntypedDF = spark.sql("select combineTitlesUntyped(title) from movies group by genre")
    titlesCombinedUntypedDF.show()

  }
}
