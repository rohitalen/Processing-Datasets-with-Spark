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
   * Genre Growth Analysis
   * Find the top 5 genres that have experienced the most growth in popularity
   * between 1990-2000 and 2010-2020 periods.
   * Only consider movies and TV shows.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the genre name
   *         - Int is the growth in number of titles
   */
  def task1(rdd: RDD[TitleBasics]): RDD[(String, Int)] = {
    // Expected output example:
    // List((Drama,3504), (Documentary,2883), (Comedy,2439), (Horror,909), (Thriller,854))
    // Filter titles for valid movies and TV shows within the specified year ranges
  val filteredTitles = rdd.filter(title =>
      title.startYear.isDefined &&
      title.titleType.exists(tt => tt == "movie" || tt == "tvSeries") && 
      title.genres.isDefined
    )
  // Assign weights based on the year range: -1 for 1990-2000 and 1 for 2010-2020
  val genreWithWeights = filteredTitles.flatMap { title =>
    val year = title.startYear.get
    val weight = if (year >= 1990 && year <= 2000) -1 else if (year >= 2010 && year <= 2020) 1 else 0

    if (weight != 0) {
      title.genres.getOrElse(List()).map(genre => (genre, weight))
    } else {
      Seq.empty
    }
  }

  val genreGrowth = genreWithWeights
    .reduceByKey(_ + _) // Sum the weights for each genre

  // Sort genres by their growth (descending) and take the top 5
  val topGenres = genreGrowth
    .sortBy(_._2, ascending = false) // Sort by growth values in descending order

  val top5Genres = topGenres.take(5)

  // Convert to RDD and return
  sc.parallelize(top5Genres)
  }

  /**
   * Genre Ratings by Decade
   * Identify top 3 genres for each decade based on weighted average ratings.
   * Exclude genres with fewer than 5 entries in each decade.
   *
   * @return RDD[(Int, List[(String, Float)])] where:
   *         - Int is the decade (e.g., 1990)
   *         - List contains tuples of (genre_name, weighted_average_rating)
   */
  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, List[(String, Float)])] = {
    // Expected output format example for one decade:
    // (1990, List((History,8.594199), (Biography,8.496143), (Drama,8.360406)))
  val numPartitions = 10 // Adjust based on your cluster configuration

  val titlesByDecadeAndGenre = l1
    .filter(title => title.startYear.getOrElse(0) >= 1900 && title.startYear.getOrElse(0) < 2030)
    .flatMap(title => title.genres.map(genre => (title.tconst, ((title.startYear.getOrElse(0) / 10) * 10, genre))))
    .partitionBy(new HashPartitioner(numPartitions)) // Repartition by `tconst` for efficient join

  val titleIDsSet = titlesByDecadeAndGenre.map(_._1).collect().toSet

  // Prepare ratings by tconst
  val ratingsByTconst = l2
    .filter(rating => titleIDsSet.contains(rating.tconst))
    .map(r => (r.tconst, (r.averageRating, r.numVotes)))

  // Join titles with ratings
  val joinedTitlesRatings = titlesByDecadeAndGenre.join(ratingsByTconst.partitionBy(new HashPartitioner(numPartitions)))

  // Calculate weighted ratings per genre for each decade
  val weightedGenreRatings = joinedTitlesRatings.flatMap { case (_, ((decade, genres), (averageRating, numVotes))) =>
    genres.map(genre => ((decade, genre), (averageRating * numVotes, numVotes, 1)))
  }

  // Aggregate weighted ratings and counts per decade and genre
  val aggregatedRatingsCounts = weightedGenreRatings.reduceByKey { case ((sumRating1, sumVotes1, count1), (sumRating2, sumVotes2, count2)) =>
    (sumRating1 + sumRating2, sumVotes1 + sumVotes2, count1 + count2)
  }

  val filteredRatings = aggregatedRatingsCounts.filter { case (_, (_, _, count)) => count >= 5 }

  // Calculate the average rating for each genre in each decade
  val averageRatingsGenre = filteredRatings.map { case ((decade, genre), (sumWeightedRating, sumNumVotes, _)) =>
    (decade, (genre, (sumWeightedRating / sumNumVotes).toFloat))
  }

  // Aggregate top 3 genres by average rating for each decade
  val topGenresByDecade = averageRatingsGenre.aggregateByKey(List.empty[(String, Float)])(
    (list, genreRating) => (genreRating :: list).sortBy(-_._2).take(3),
    (list1, list2) => (list1 ++ list2).sortBy(-_._2).take(3)
  )

  topGenresByDecade.sortByKey()
  }

  /**
   * Top Directors
   * Find directors with highest average ratings for their films.
   * Consider only directors with at least 3 films rated above 8.5 with 10,000+ votes.
   *
   * @return RDD[(String, Float)] where:
   *         - String is the director's name
   *         - Float is their average rating
   */
  def task3(l1: RDD[NameBasics], l2: RDD[TitleRatings], l3: RDD[TitleCrew]): RDD[(String, Float)] = {
    // Expected output example:
    // List((Tetsurô Araki,9.2), (Matt Shakman,9.066667))
  val numPartitions = 10
  val highRatedTitles = l2
    .filter(rating => rating.averageRating > 8.5 && rating.numVotes >= 10000)
    .map(rating => (rating.tconst, rating.averageRating)) // (tconst, averageRating)

  // Map TitleCrew to (tconst, directorId)
  val titleToDirectors = l3
    .filter(crew => crew.directors.isDefined) 
    .flatMap(crew => crew.directors.get.map(directorId => (crew.tconst, directorId))) 

  // Join highRatedTitles with titleToDirectors
  val directorRatings = highRatedTitles
    .join(titleToDirectors) 
    .map { case (_, (averageRating, directorId)) => (directorId, (averageRating, 1)) }.partitionBy(new HashPartitioner(numPartitions)) // (directorId, (rating, 1))

  val aggregatedRatings = directorRatings.reduceByKey({ case ((sumRatings1, count1), (sumRatings2, count2)) =>
    (sumRatings1 + sumRatings2, count1 + count2)
  }) 
  // Filter directors with at least 3 movies and calculate average rating
  val directorsWithRatings = aggregatedRatings
    .filter { case (_, (_, count)) => count >= 3 } 
    .mapValues { case (sumRatings, count) => sumRatings / count } 

  val directorIdToName = l1
    .filter(name => name.primaryName.isDefined) 
    .map(name => (name.nconst, name.primaryName.get)) 

  // Join directorsWithRatings with director names
  val directorsWithNamesAndRatings = directorsWithRatings
    .join(directorIdToName) 
    .map { case (_, (averageRating, directorName)) => (directorName, averageRating) } 

  // Sort by average rating (descending), then by name (alphabetically)
  val sortedDirectors = directorsWithNamesAndRatings
    .sortBy({ case (directorName, averageRating) => (-averageRating, directorName) })

  val topDirectors = sortedDirectors.take(10)

  val topDirectorsRDD = sc.parallelize(topDirectors)

  topDirectorsRDD
  }

  /**
   * Prolific Crew Members
   * Identify top 10 most prolific crew members across specific time periods.
   * Consider contributions in 1995-2000, 2005-2010, and 2015-2020.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the crew member's name
   *         - Int is their total number of unique contributions
   */
  def task4(l1: RDD[TitleBasics], l2: RDD[NameBasics], l3: RDD[TitleCrew]): RDD[(String, Int)] = {
    // Expected output example:
    // List((Marco Romano,12), (Jordan Hill,12), ..., (Tony Newton,8))
  val numPartitions = 10
  val filteredMovies = l1.filter { movie =>
  movie.startYear.exists(year => (year >= 1995 && year <= 2000) ||
                                  (year >= 2005 && year <= 2010) ||
                                  (year >= 2015 && year <= 2020))
  }.map(movie => (movie.tconst, movie.startYear))

  // Create a set of filtered movie IDs for efficient lookup
  val filteredMovieIds = filteredMovies.map(_._1).collect().toSet

  // Filter crew data to include only valid movies
  val relevantCrew = l3.filter(crew => filteredMovieIds.contains(crew.tconst))

  val crewMoviePairs = relevantCrew.flatMap { crew =>
    val directorMoviePairs = crew.directors.getOrElse(List.empty).map(directorId => (directorId, crew.tconst))
    val writerMoviePairs = crew.writers.getOrElse(List.empty).map(writerId => (writerId, crew.tconst))
    directorMoviePairs ::: writerMoviePairs
  }.distinct() 

  val crewMovieCounts = crewMoviePairs
    .map { case (nconst, _) => (nconst, 1) }
    .reduceByKey(_ + _) // Aggregate counts by crew member

  // Map crew IDs (nconst) to their names
  val crewIdToNameRDD = l2
    .map(name => (name.nconst, name.primaryName.getOrElse("")))
    .partitionBy(new HashPartitioner(numPartitions))

  // Join crew movie counts with crew names
  val crewWithNamesAndCounts = crewMovieCounts.join(crewIdToNameRDD)

  val sortedCrewWithCounts = crewWithNamesAndCounts
    .map { case (_, (count, name)) => (name, count) }
    .sortBy { case (name, count) => (-count, name) }

  // Collect the top 10 results and convert to an RDD
  val topCrewList = sortedCrewWithCounts.take(10)
  val topCrewRDD = sc.parallelize(topCrewList)

  topCrewRDD
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }
}