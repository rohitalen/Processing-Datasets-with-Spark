package imdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object ImdbSpark {
  val conf: SparkConf = new SparkConf()
    .setAppName("ImdbAnalysis")
    .setMaster("local[*]")
    .set("spark.ui.enabled", "false")

  val sc: SparkContext = new SparkContext(conf)

  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath)
    .map(line => ImdbData.parseTitleBasics(line))

  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath)
    .map(line => ImdbData.parseTitleRatings(line))

  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath)
    .map(line => ImdbData.parseTitleCrew(line))

  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath)
    .map(line => ImdbData.parseNameBasics(line))

  def main(args: Array[String]) {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    println(durations)

    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    println(titles)

    val topRated = timed("Task 3", task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList)
    println(topRated)

    val crews = timed("Task 4", task4(titleBasicsRDD, nameBasicsRDD, titleCrewRDD).collect().toList)
    println(crews)

    sc.stop()
  }

  /**
   * Task 1: Genre Growth Analysis
   * Find the top 5 genres that have experienced the most growth in popularity
   * between 1990-2000 and 2010-2020 periods.
   * Only consider movies and TV shows.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the genre name
   *         - Int is the growth in number of titles
   */
  def task1(rdd: RDD[TitleBasics]): RDD[(String, Int)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Drama,3504), (Documentary,2883), (Comedy,2439), (Horror,909), (Thriller,854))
    rdd
    // Step 1: Filter titles
    .filter(title =>
      title.startYear.isDefined &&
      (title.titleType.contains("movie") || title.titleType.contains("tvSeries")) &&
      title.genres.isDefined
    )
    // Step 2: Map each genre to a (genre, timePeriod) pair with a count of 1
    .flatMap(title => {
      val period = title.startYear.get match {
        case year if 1990 <= year && year <= 2000 => Some("1990-2000")
        case year if 2010 <= year && year <= 2020 => Some("2010-2020")
        case _ => None
      }
      period match {
        case Some(p) => title.genres.get.map(genre => ((genre, p), 1))
        case None => Seq.empty
      }
    })
    // Step 3: Aggregate counts
    .reduceByKey(_ + _)
    // Step 4: Transform to (genre, (count1990_2000, count2010_2020))
    .map { case ((genre, period), count) =>
      (genre, period match {
        case "1990-2000" => (count, 0)
        case "2010-2020" => (0, count)
      })
    }
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)) // Combine counts for each genre
    // Step 5: Calculate growth
    .mapValues { case (count1990_2000, count2010_2020) =>
      count2010_2020 - count1990_2000
    }
    // Step 6: Sort and limit to top 5 genres
    .sortBy(_._2, ascending = false)
    .zipWithIndex() // Add index to retain top 5
    .filter(_._2 < 5) // Filter only the top 5
    .map(_._1) // Remove the index
  }

  /**
   * Task 2: Genre Ratings by Decade
   * Identify top 3 genres for each decade based on weighted average ratings.
   * Exclude genres with fewer than 5 entries in each decade.
   *
   * @return RDD[(Int, List[(String, Float)])] where:
   *         - Int is the decade (e.g., 1990)
   *         - List contains tuples of (genre_name, weighted_average_rating)
   */
  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, List[(String, Float)])] = {
    // TODO: Implement this task
    // Expected output format example for one decade:
    // (1990, List((History,8.594199), (Biography,8.496143), (Drama,8.360406)))
    ???
  }

  /**
   * Task 3: Top Directors
   * Find directors with highest average ratings for their films.
   * Consider only directors with at least 3 films rated above 8.5 with 10,000+ votes.
   *
   * @return RDD[(String, Float)] where:
   *         - String is the director's name
   *         - Float is their average rating
   */
  def task3(l1: RDD[NameBasics], l2: RDD[TitleRatings], l3: RDD[TitleCrew]): RDD[(String, Float)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Tetsurô Araki,9.2), (Matt Shakman,9.066667))
    ???
  }

  /**
   * Task 4: Prolific Crew Members
   * Identify top 10 most prolific crew members across specific time periods.
   * Consider contributions in 1995-2000, 2005-2010, and 2015-2020.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the crew member's name
   *         - Int is their total number of unique contributions
   */
  def task4(l1: RDD[TitleBasics], l2: RDD[NameBasics], l3: RDD[TitleCrew]): RDD[(String, Int)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Marco Romano,12), (Jordan Hill,12), ..., (Tony Newton,8))
    ???
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }
}