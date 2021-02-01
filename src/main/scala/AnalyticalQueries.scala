import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, current_timestamp, datediff, explode, from_unixtime, lit, max, regexp_extract, split, year}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object AnalyticalQueries extends App {

  val spark = SparkSession.builder().master("local[1]").getOrCreate();

  val moviesSchema = StructType(Array(StructField("MovieID",IntegerType,false),
    StructField("Title",StringType,true),
    StructField("Genres",StringType,true)))


  val moviesDf = spark.read.format("csv").
                  option("sep","::").
                  option("header",false).
                  option("path", "src/main/resources/movies.dat").
                  schema(moviesSchema).
                  load()

  val ratingsSchema = StructType(Array(
    StructField("UserID",IntegerType,false),
    StructField("MovieID",IntegerType,true),
    StructField("Rating",IntegerType,true),
    StructField("TimeStamp",LongType,true)))

  val ratingsDf = spark.read.format("csv").
    option("sep","::").
    option("header",false).
    option("path", "src/main/resources/ratings.dat").
    schema(ratingsSchema).
    load()

  val usersSchema = StructType(Array(
    StructField("UserID",IntegerType,false),
    StructField("Gender",StringType,true),
    StructField("Age",IntegerType,true),
    StructField("Occupation",IntegerType,true),
    StructField("ZipCode",IntegerType,true)
  ))

  val usersDf = spark.read.format("csv").
    option("sep","::").
    option("header",false).
    option("path", "src/main/resources/users.dat").
    schema(usersSchema).
    load()

  //  Ten most viewed movies

  val tenMostViewedMovies = ratingsDf.
    withColumnRenamed("MovieID","Movie").
    groupBy(col("Movie")).
    agg(count("*").alias("movieCount")).
    join(moviesDf,(col("Movie") === moviesDf.col("MovieID"))).
    sort(col("movieCount").desc).
    limit(10).
    select("Title","movieCount").
    coalesce(1)

  tenMostViewedMovies.write.
    mode(SaveMode.Overwrite).
    csv("src/main/resources/top10movies.csv")

  // Distinct genres available

  val distinctGenres = moviesDf.
    withColumn("flattenGenres",explode(split(col("Genres"),"\\|"))).
    select("flattenGenres").distinct().repartition(1)

  distinctGenres.write.mode(SaveMode.Overwrite).csv("src/main/resources/distinctGenres.csv")

  // How many movies for each genre?
  val countMoviesPerGenre = moviesDf.
                            withColumn("flattenGenres",explode(split(col("Genres"),"\\|"))).
                            groupBy("flattenGenres").count().coalesce(1)

  countMoviesPerGenre.write.mode(SaveMode.Overwrite).csv("src/main/resources/countMoviesPerGenre.csv")

  // Latest movies released

  val yearOfRecentMovie = moviesDf.withColumn("year",regexp_extract(col("Title"),"\\(([0-9]+)\\)",1)).
                  agg(max("year").cast("int")).head().getInt(0)

  val recentMovies = moviesDf.withColumn("year",regexp_extract(col("Title"),"\\(([0-9]+)\\)",1)).
                    filter(col("year") === lit(yearOfRecentMovie)).select("MovieID","year")

  recentMovies.write.mode(SaveMode.Overwrite).csv("src/main/resources/recentMovies.csv")
}
