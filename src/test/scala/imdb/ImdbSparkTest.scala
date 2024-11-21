package imdb

import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.{Try, Success, Failure}


class ImdbSparkTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var titleBasicsRDD: RDD[TitleBasics] = _
  private var titleRatingsRDD: RDD[TitleRatings] = _
  private var titleCrewRDD: RDD[TitleCrew] = _
  private var nameBasicsRDD: RDD[NameBasics] = _
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Configure logging
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    // Initialize RDDs
    titleBasicsRDD = ImdbSpark.titleBasicsRDD
    titleRatingsRDD = ImdbSpark.titleRatingsRDD
    titleCrewRDD = ImdbSpark.titleCrewRDD
    nameBasicsRDD = ImdbSpark.nameBasicsRDD
    sc = ImdbSpark.sc
  }

  override def afterAll(): Unit = {
    SparkContextWrapper.stop()
    super.afterAll()
  }

  private def printTestStatus(taskName: String, executionTime: Long, passed: Boolean): Unit = {
    val statusSymbol = if (passed) "✓" else "✗"
    println(s"""
      |----------------------------------------
      |Test Results for $taskName:
      |Status: $statusSymbol ${if (passed) "PASSED" else "FAILED"}
      |Execution Time: ${executionTime}ms
      |----------------------------------------
      |""".stripMargin)
  }

  "Task1: Genre Growth Analysis" should "find the correct top 5 genres with growth" in {
    val expected = List(
      ("Drama", 3504),
      ("Documentary", 2883),
      ("Comedy", 2439),
      ("Horror", 909),
      ("Thriller", 854)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(titleBasicsRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == 5
    val correctContent = result == expected

    val passed = correctLength && correctContent
    printTestStatus("Task 1", executionTime, passed)

    if (!correctLength) println(s"Expected length: 5, but got: ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }

    result should have length 5
    result should contain theSameElementsInOrderAs expected
  }

  it should "return an empty RDD when given an empty input RDD" in {
    val testName = "Task 1: Empty Input RDD"

    val emptyRDD: RDD[TitleBasics] = sc.emptyRDD[TitleBasics]

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(emptyRDD).collect()
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result.isEmpty
    printTestStatus(testName, executionTime, passed)

    result shouldBe empty
  }

  it should "return an empty RDD when no titles are in the target years" in {
    val testName = "Task 1: Titles Outside Target Years"

    val testData = Seq(
      TitleBasics("tt0000001", Some("movie"), Some("Title1"), Some("OriginalTitle1"), 0, Some(1985), None, None, Some(List("Drama")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect()
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result.isEmpty
    printTestStatus(testName, executionTime, passed)

    result shouldBe empty
  }

  it should "handle missing optional fields gracefully" in {
    val testName = "Task 1: Handling Missing Optional Fields"

    val testData = Seq(
      TitleBasics("tt0000002", None, Some("Title2"), Some("OriginalTitle2"), 0, Some(1995), None, None, Some(List("Comedy"))),
      TitleBasics("tt0000003", Some("movie"), Some("Title3"), Some("OriginalTitle3"), 0, None, None, None, Some(List("Drama"))),
      TitleBasics("tt0000004", Some("movie"), Some("Title4"), Some("OriginalTitle4"), 0, Some(2015), None, None, None)
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect()
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result.isEmpty
    printTestStatus(testName, executionTime, passed)

    result shouldBe empty
  }

  it should "correctly count genres when titles have multiple genres" in {
    val testName = "Task 1: Multiple Genres per Title"

    val testData = Seq(
      TitleBasics("tt0000005", Some("movie"), Some("Title5"), Some("OriginalTitle5"), 0, Some(1995), None, None, Some(List("Drama", "Comedy"))),
      TitleBasics("tt0000006", Some("movie"), Some("Title6"), Some("OriginalTitle6"), 0, Some(2015), None, None, Some(List("Drama", "Action")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect().toMap
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val expected = Map("Drama" -> 0, "Comedy" -> -1, "Action" -> 1)
    val passed = result == expected
    printTestStatus(testName, executionTime, passed)

    result should contain allElementsOf expected

  }

   it should "handle zero counts correctly" in {
    val testName = "Task 1: Zero Counts Handling"

    val testData = Seq(
      TitleBasics("tt0000007", Some("movie"), Some("Title7"), Some("OriginalTitle7"), 0, Some(1995), None, None, Some(List("Mystery"))),
      TitleBasics("tt0000008", Some("movie"), Some("Title8"), Some("OriginalTitle8"), 0, Some(1996), None, None, Some(List("Mystery"))),
      TitleBasics("tt0000009", Some("movie"), Some("Title9"), Some("OriginalTitle9"), 0, Some(2015), None, None, Some(List("Sci-Fi")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect().toMap
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val expected = Map("Mystery" -> -2, "Sci-Fi" -> 1)
    val passed = result == expected
    printTestStatus(testName, executionTime, passed)

    result should contain allElementsOf expected
  }

  it should "correctly compute negative growth counts" in {
    val testName = "Task 1: Negative Growth Counts"

    val testData = Seq(
      TitleBasics("tt0000010", Some("movie"), Some("Title10"), Some("OriginalTitle10"), 0, Some(1995), None, None, Some(List("Horror"))),
      TitleBasics("tt0000011", Some("movie"), Some("Title11"), Some("OriginalTitle11"), 0, Some(1995), None, None, Some(List("Horror"))),
      TitleBasics("tt0000012", Some("movie"), Some("Title12"), Some("OriginalTitle12"), 0, Some(2015), None, None, Some(List("Horror")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect().toMap
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val expected = Map("Horror" -> -1)
    val passed = result == expected
    printTestStatus(testName, executionTime, passed)

    result should contain ("Horror" -> -1)
  }

  it should "handle ties in growth counts correctly" in {
    val testName = "Task 1: Ties in Growth Counts"

    val testData = Seq(
      TitleBasics("tt0000013", Some("movie"), Some("Title13"), Some("OriginalTitle13"), 0, Some(1995), None, None, Some(List("Animation"))),
      TitleBasics("tt0000014", Some("movie"), Some("Title14"), Some("OriginalTitle14"), 0, Some(2015), None, None, Some(List("Animation"))),
      TitleBasics("tt0000015", Some("movie"), Some("Title15"), Some("OriginalTitle15"), 0, Some(1995), None, None, Some(List("Adventure"))),
      TitleBasics("tt0000016", Some("movie"), Some("Title16"), Some("OriginalTitle16"), 0, Some(2015), None, None, Some(List("Adventure")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect().toMap
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val expected = Map("Animation" -> 0, "Adventure" -> 0)
    val passed = result == expected
    printTestStatus(testName, executionTime, passed)

    result should contain allElementsOf expected
  }

  it should "exclude titles that are not 'movie' or 'tvSeries'" in {
    val testName = "Task 1: Exclude Non-Target Title Types"

    val testData = Seq(
      TitleBasics("tt0000019", Some("short"), Some("Title19"), Some("OriginalTitle19"), 0, Some(1995), None, None, Some(List("Comedy"))),
      TitleBasics("tt0000020", Some("tvEpisode"), Some("Title20"), Some("OriginalTitle20"), 0, Some(2015), None, None, Some(List("Comedy")))
    )
    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect()
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result.isEmpty
    printTestStatus(testName, executionTime, passed)

    result shouldBe empty
  }

  it should "handle a large number of genres and return the top 5 correctly" in {
    val testName = "Task 1: Large Dataset Handling"

    val genres = List("Drama", "Comedy", "Action", "Thriller", "Horror", "Documentary", "Sci-Fi", "Fantasy", "Mystery", "Romance")

    // Adjust test data to create varying growth counts
    val testData = (1 to 1000).map { i =>
      val genreIndex = i % genres.length
      val genre = genres(genreIndex)
      val year = if (genreIndex < 5) {
        // First 5 genres have more titles in 2015
        if (i % 3 == 0) 1995 else 2015
      } else {
        // Other genres have more titles in 1995
        if (i % 3 == 0) 2015 else 1995
      }
      TitleBasics(
        s"tt${1000000 + i}",
        Some("movie"),
        Some(s"Title$i"),
        Some(s"OriginalTitle$i"),
        0,
        Some(year),
        None,
        None,
        Some(List(genre))
      )
    }

    val rdd = sc.parallelize(testData)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task1(rdd).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Compute expected growth per genre
    val counts = scala.collection.mutable.Map[(String, String), Int]().withDefaultValue(0)
    testData.foreach { title =>
      val genre = title.genres.get.head
      val year = title.startYear.get
      val period = if (year >= 1990 && year <= 2000) "1990s"
                  else if (year >= 2010 && year <= 2020) "2010s"
                  else "other"

      if (period != "other") {
        counts((genre, period)) += 1
      }
    }

    val growthPerGenre = genres.map { genre =>
      val count1990s = counts((genre, "1990s"))
      val count2010s = counts((genre, "2010s"))
      val growth = count2010s - count1990s
      (genre, growth)
    }

    // Sort by growth descending, then by genre name ascending
    val expectedTop5 = growthPerGenre.sortBy { case (genre, growth) => (-growth, genre) }.take(5)

    // Similarly, sort the result in the same way
    val sortedResult = result.sortBy { case (genre, growth) => (-growth, genre) }

    // Determine if the test passed
    val passed = Try {
      sortedResult should have length 5
      sortedResult shouldEqual expectedTop5
    } match {
      case Success(_) => true
      case Failure(e) =>
        println(s"Test failed with exception: $e")
        false
    }

    // Print test status
    printTestStatus(testName, executionTime, passed)

    // Ensure the test fails if assertions did not pass
    passed shouldBe true
  }

  "Task2: Genre Ratings by Decade" should "calculate correct weighted averages per decade" in {
    val expected = List(
      (1900, List(("Music", 6.597943f), ("Crime", 6.159037f), ("Biography", 6.0411067f))),
      (1910, List(("War", 7.1199822f), ("Horror", 6.914935f), ("Thriller", 6.7857585f))),
      (1920, List(("Sci-Fi", 8.284826f), ("Family", 8.154517f), ("Action", 8.095828f))),
      (1930, List(("Thriller", 8.187101f), ("Mystery", 7.9939985f), ("Crime", 7.950047f))),
      (1940, List(("Fantasy", 8.493501f), ("Family", 8.429712f), ("War", 8.406213f))),
      (1950, List(("Crime", 8.504564f), ("Thriller", 8.394195f), ("Mystery", 8.336316f))),
      (1960, List(("Western", 8.649012f), ("Adventure", 8.299024f), ("Thriller", 8.269708f))),
      (1970, List(("Crime", 8.794421f), ("Drama", 8.321298f), ("Sci-Fi", 8.06072f))),
      (1980, List(("War", 8.122867f), ("Western", 7.992603f), ("Sci-Fi", 7.978767f))),
      (1990, List(("History", 8.594199f), ("Biography", 8.496143f), ("Drama", 8.360406f))),
      (2000, List(("History", 8.6177435f), ("War", 8.295243f), ("Drama", 8.035681f))),
      (2010, List(("Western", 8.212091f), ("History", 7.9701552f), ("Animation", 7.8727164f))),
      (2020, List(("Animation", 7.9739513f), ("Adventure", 7.5102105f), ("Action", 7.332238f)))
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task2(titleBasicsRDD, titleRatingsRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    var allMatches = true
    var mismatchDetails = new StringBuilder()

    result.zip(expected).foreach { case ((decadeR, genresR), (decadeE, genresE)) =>
      if (decadeR != decadeE) {
        allMatches = false
        mismatchDetails.append(s"\nDecade mismatch: Expected $decadeE, Got $decadeR")
      }
      if (genresR.length != 3) {
        allMatches = false
        mismatchDetails.append(s"\nIncorrect number of genres for decade $decadeR: Expected 3, Got ${genresR.length}")
      }
      genresR.zip(genresE).foreach { case ((genreR, ratingR), (genreE, ratingE)) =>
        if (genreR != genreE) {
          allMatches = false
          mismatchDetails.append(s"\nGenre mismatch in decade $decadeR: Expected $genreE, Got $genreR")
        }
        if (math.abs(ratingR - ratingE) > 0.0001f) {
          allMatches = false
          mismatchDetails.append(s"\nRating mismatch for $genreR in decade $decadeR: Expected $ratingE, Got $ratingR")
        }
      }
    }

    val passed = allMatches && result.length == expected.length
    printTestStatus("Task 2", executionTime, passed)

    if (!allMatches) println(s"Mismatches found: $mismatchDetails")
    if (result.length != expected.length) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")

    // Original assertions
    result.length shouldBe expected.length
    result.zip(expected).foreach { case ((decadeR, genresR), (decadeE, genresE)) =>
      decadeR shouldBe decadeE
      genresR.length shouldBe 3
      genresR.zip(genresE).foreach { case ((genreR, ratingR), (genreE, ratingE)) =>
        genreR shouldBe genreE
        ratingR shouldBe (ratingE +- 0.0001f)
      }
    }
  }

  it should "exclude genres with fewer than 5 entries per decade" in {
    val testName = "Task 2: Exclude Genres with <5 Entries"

    // Titles with fewer than 5 entries for certain genres
    val titles = Seq(
      TitleBasics("tt0000010", Some("movie"), Some("Title10"), Some("OriginalTitle10"), 0, Some(1920), None, None, Some(List("Horror"))),
      TitleBasics("tt0000011", Some("movie"), Some("Title11"), Some("OriginalTitle11"), 0, Some(1920), None, None, Some(List("Horror"))),
      TitleBasics("tt0000012", Some("movie"), Some("Title12"), Some("OriginalTitle12"), 0, Some(1920), None, None, Some(List("Horror"))),
      TitleBasics("tt0000013", Some("movie"), Some("Title13"), Some("OriginalTitle13"), 0, Some(1920), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000014", Some("movie"), Some("Title14"), Some("OriginalTitle14"), 0, Some(1920), None, None, Some(List("Sci-Fi")))
    )
    val ratings = Seq(
      TitleRatings("tt0000010", 7.0f, 100),
      TitleRatings("tt0000011", 8.0f, 150),
      TitleRatings("tt0000012", 6.5f, 200),
      TitleRatings("tt0000013", 9.0f, 300),
      TitleRatings("tt0000014", 8.5f, 250)
    )

    val titlesRDD = sc.parallelize(titles)
    val ratingsRDD = sc.parallelize(ratings)

    val expected = List() // No genres should be included

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task2(titlesRDD, ratingsRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = Try {
      result shouldBe empty
    } match {
      case Success(_) => true
      case Failure(e) =>
        println(s"Test failed with exception: $e")
        false
    }

    printTestStatus(testName, executionTime, passed)
    passed shouldBe true
  }

  it should "handle missing optional fields gracefully" in {
    val testName = "Task 2: Handling Missing Optional Fields"

    val titles = Seq(
      TitleBasics("tt0000020", None, Some("Title20"), Some("OriginalTitle20"), 0, Some(1930), None, None, Some(List("Comedy"))),
      TitleBasics("tt0000021", Some("movie"), Some("Title21"), Some("OriginalTitle21"), 0, None, None, None, Some(List("Drama"))),
      TitleBasics("tt0000022", Some("movie"), Some("Title22"), Some("OriginalTitle22"), 0, Some(1930), None, None, None)
    )
    val ratings = Seq(
      TitleRatings("tt0000020", 7.5f, 120),
      TitleRatings("tt0000021", 8.0f, 80),
      TitleRatings("tt0000022", 6.5f, 60)
    )

    val titlesRDD = sc.parallelize(titles)
    val ratingsRDD = sc.parallelize(ratings)

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task2(titlesRDD, ratingsRDD).collect()
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = Try {
      result shouldBe empty
    } match {
      case Success(_) => true
      case Failure(e) =>
        println(s"Test failed with exception: $e")
        false
    }

    printTestStatus(testName, executionTime, passed)
    passed shouldBe true
  }

  it should "correctly calculate weighted averages with multiple genres per title" in {
    val testName = "Task 2: Multiple Genres per Title"

    val titles = Seq(
      TitleBasics("tt0000030", Some("movie"), Some("Title30"), Some("OriginalTitle30"), 0, Some(1940), None, None, Some(List("Drama", "Action"))),
      TitleBasics("tt0000031", Some("movie"), Some("Title31"), Some("OriginalTitle31"), 0, Some(1940), None, None, Some(List("Drama", "Thriller"))),
      TitleBasics("tt0000032", Some("movie"), Some("Title32"), Some("OriginalTitle32"), 0, Some(1940), None, None, Some(List("Action", "Thriller"))),
      TitleBasics("tt0000033", Some("movie"), Some("Title33"), Some("OriginalTitle33"), 0, Some(1940), None, None, Some(List("Drama", "Action"))),
      TitleBasics("tt0000034", Some("movie"), Some("Title34"), Some("OriginalTitle34"), 0, Some(1940), None, None, Some(List("Thriller"))),
      // Adding more titles to reach at least 5 entries per genre
      TitleBasics("tt0000035", Some("movie"), Some("Title35"), Some("OriginalTitle35"), 0, Some(1940), None, None, Some(List("Drama", "Action"))),
      TitleBasics("tt0000036", Some("movie"), Some("Title36"), Some("OriginalTitle36"), 0, Some(1940), None, None, Some(List("Drama", "Thriller"))),
      TitleBasics("tt0000037", Some("movie"), Some("Title37"), Some("OriginalTitle37"), 0, Some(1940), None, None, Some(List("Action", "Thriller"))),
      TitleBasics("tt0000038", Some("movie"), Some("Title38"), Some("OriginalTitle38"), 0, Some(1940), None, None, Some(List("Drama", "Action"))),
      TitleBasics("tt0000039", Some("movie"), Some("Title39"), Some("OriginalTitle39"), 0, Some(1940), None, None, Some(List("Thriller")))
    )
    val ratings = Seq(
      TitleRatings("tt0000030", 8.0f, 100),
      TitleRatings("tt0000031", 7.5f, 150),
      TitleRatings("tt0000032", 9.0f, 200),
      TitleRatings("tt0000033", 8.5f, 120),
      TitleRatings("tt0000034", 7.0f, 80),
      TitleRatings("tt0000035", 8.2f, 110),
      TitleRatings("tt0000036", 7.7f, 130),
      TitleRatings("tt0000037", 8.9f, 190),
      TitleRatings("tt0000038", 8.4f, 115),
      TitleRatings("tt0000039", 7.1f, 85)
    )

    val titlesRDD = sc.parallelize(titles)
    val ratingsRDD = sc.parallelize(ratings)

    // Calculate expected weighted averages
    val ratingsMap = ratings.map(r => (r.tconst, (r.averageRating, r.numVotes))).toMap

    val genres = List("Action", "Drama", "Thriller")
    val genreRatingsMap = genres.map { genre =>
      val ratingsList = titles.filter(_.genres.get.contains(genre)).map { title =>
        ratingsMap(title.tconst)
      }
      val totalWeightedRating = ratingsList.map { case (rating, numVotes) => rating * numVotes }.sum
      val totalNumVotes = ratingsList.map { case (_, numVotes) => numVotes }.sum
      val weightedAverage = (totalWeightedRating / totalNumVotes).toFloat
      (genre, weightedAverage)
    }.toMap

    // Sort genres by weighted average descending
    val expectedGenres = genreRatingsMap.toList.sortBy(-_._2).take(3)

    val expected = List((1940, expectedGenres))

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task2(titlesRDD, ratingsRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = Try {
      result should have length expected.length
      result.zip(expected).foreach { case ((decadeR, genresR), (decadeE, genresE)) =>
        decadeR shouldBe decadeE
        genresR should have length genresE.length
        genresR.zip(genresE).foreach { case ((genreR, ratingR), (genreE, ratingE)) =>
          genreR shouldBe genreE
          ratingR shouldBe (ratingE +- 0.0001f)
        }
      }
    } match {
      case Success(_) => true
      case Failure(e) =>
        println(s"Test failed with exception: $e")
        false
    }

    printTestStatus(testName, executionTime, passed)
    passed shouldBe true
  }

  it should "correctly calculate decades from startYear" in {
    val testName = "Task 2: Correct Decade Calculation"

    val titles = Seq(
      TitleBasics("tt0000040", Some("movie"), Some("Title40"), Some("OriginalTitle40"), 0, Some(1989), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000041", Some("movie"), Some("Title41"), Some("OriginalTitle41"), 0, Some(1990), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000042", Some("movie"), Some("Title42"), Some("OriginalTitle42"), 0, Some(1999), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000043", Some("movie"), Some("Title43"), Some("OriginalTitle43"), 0, Some(2000), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000044", Some("movie"), Some("Title44"), Some("OriginalTitle44"), 0, Some(2001), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000045", Some("movie"), Some("Title45"), Some("OriginalTitle45"), 0, Some(1990), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000046", Some("movie"), Some("Title46"), Some("OriginalTitle46"), 0, Some(1999), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000047", Some("movie"), Some("Title47"), Some("OriginalTitle47"), 0, Some(2000), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000048", Some("movie"), Some("Title48"), Some("OriginalTitle48"), 0, Some(2001), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000049", Some("movie"), Some("Title49"), Some("OriginalTitle49"), 0, Some(1990), None, None, Some(List("Sci-Fi"))),
      TitleBasics("tt0000050", Some("movie"), Some("Title50"), Some("OriginalTitle50"), 0, Some(2002), None, None, Some(List("Sci-Fi")))
    )
    val ratings = Seq(
      TitleRatings("tt0000040", 7.0f, 100),
      TitleRatings("tt0000041", 8.0f, 150),
      TitleRatings("tt0000042", 9.0f, 200),
      TitleRatings("tt0000043", 8.5f, 120),
      TitleRatings("tt0000044", 7.5f, 80),
      TitleRatings("tt0000045", 8.0f, 110),
      TitleRatings("tt0000046", 8.5f, 90),
      TitleRatings("tt0000047", 7.0f, 130),
      TitleRatings("tt0000048", 8.0f, 140),
      TitleRatings("tt0000049", 7.5f, 160),
      TitleRatings("tt0000050", 7.8f, 100)
    )

    val titlesRDD = sc.parallelize(titles)
    val ratingsRDD = sc.parallelize(ratings)

    val expected = List(
      (1990, List(("Sci-Fi", 8.232394f))),
      (2000, List(("Sci-Fi", 7.77193f)))
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task2(titlesRDD, ratingsRDD).collect().toList.sortBy(_._1)
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = Try {
      result should have length expected.length
      result.zip(expected).foreach { case ((decadeR, genresR), (decadeE, genresE)) =>
        decadeR shouldBe decadeE
        genresR should have length genresE.length
        genresR.zip(genresE).foreach { case ((genreR, ratingR), (genreE, ratingE)) =>
          genreR shouldBe genreE
          ratingR shouldBe (ratingE +- 0.0001f)
        }
      }
    } match {
      case Success(_) => true
      case Failure(e) =>
        println(s"Test failed with exception: $e")
        false
    }

    printTestStatus(testName, executionTime, passed)
    passed shouldBe true
  }

  "Task3: Top Directors" should "find directors with highest rated films" in {
    val expected = List(
      ("Tetsurô Araki", 9.2f),
      ("Matt Shakman", 9.066667f)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == 2
    var correctContent = true
    var mismatchDetails = new StringBuilder()

    result.zip(expected).foreach { case ((nameR, ratingR), (nameE, ratingE)) =>
      if (nameR != nameE) {
        correctContent = false
        mismatchDetails.append(s"\nName mismatch: Expected $nameE, Got $nameR")
      }
      if (math.abs(ratingR - ratingE) > 0.0001f) {
        correctContent = false
        mismatchDetails.append(s"\nRating mismatch for $nameR: Expected $ratingE, Got $ratingR")
      }
    }

    val passed = correctLength && correctContent
    printTestStatus("Task 3", executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected 2, Got ${result.length}")
    if (!correctContent) println(s"Mismatches found: $mismatchDetails")

    // Original assertions
    result should have length 2
    result.zip(expected).foreach { case ((nameR, ratingR), (nameE, ratingE)) =>
      nameR shouldBe nameE
      ratingR shouldBe (ratingE +- 0.0001f)
    }
  }

  "Task3: Top Directors" should "correctly compute average ratings for directors with at least three qualifying films" in {
    val testName = "Task 3: Basic Functionality"

    // Prepare test data
    val nameBasics = Seq(
      NameBasics("nm0000001", Some("Director One"), None, None, None, None),
      NameBasics("nm0000002", Some("Director Two"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000001", 9.0f, 15000),
      TitleRatings("tt0000002", 8.6f, 20000),
      TitleRatings("tt0000003", 8.8f, 12000),
      TitleRatings("tt0000004", 8.9f, 18000),
      TitleRatings("tt0000005", 9.1f, 16000),
      TitleRatings("tt0000006", 8.7f, 11000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000001", Some(List("nm0000001")), None),
      TitleCrew("tt0000002", Some(List("nm0000001")), None),
      TitleCrew("tt0000003", Some(List("nm0000001")), None),
      TitleCrew("tt0000004", Some(List("nm0000002")), None),
      TitleCrew("tt0000005", Some(List("nm0000002")), None),
      TitleCrew("tt0000006", Some(List("nm0000002")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Director Two", (8.9f + 9.1f + 8.7f) / 3),
      ("Director One", (9.0f + 8.6f + 8.8f) / 3)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "exclude directors with less than three qualifying films" in {
    val testName = "Task 3: Exclude Directors with <3 Films"

    val nameBasics = Seq(
      NameBasics("nm0000010", Some("Director Three"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000010", 9.0f, 15000),
      TitleRatings("tt0000011", 8.6f, 20000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000010", Some(List("nm0000010")), None),
      TitleCrew("tt0000011", Some(List("nm0000010")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List.empty[(String, Float)]

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "ignore films with ratings below or equal to 8.5" in {
    val testName = "Task 3: Ignore Films Below Rating Threshold"

    val nameBasics = Seq(
      NameBasics("nm0000020", Some("Director Four"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000020", 8.5f, 15000),
      TitleRatings("tt0000021", 8.4f, 20000),
      TitleRatings("tt0000022", 9.0f, 15000),
      TitleRatings("tt0000023", 9.1f, 16000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000020", Some(List("nm0000020")), None),
      TitleCrew("tt0000021", Some(List("nm0000020")), None),
      TitleCrew("tt0000022", Some(List("nm0000020")), None),
      TitleCrew("tt0000023", Some(List("nm0000020")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List.empty[(String, Float)]

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "ignore films with numVotes below 10,000" in {
    val testName = "Task 3: Ignore Films Below NumVotes Threshold"

    val nameBasics = Seq(
      NameBasics("nm0000030", Some("Director Five"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000030", 9.0f, 9500),
      TitleRatings("tt0000031", 8.6f, 8000),
      TitleRatings("tt0000032", 9.1f, 7000),
      TitleRatings("tt0000033", 9.2f, 11000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000030", Some(List("nm0000030")), None),
      TitleCrew("tt0000031", Some(List("nm0000030")), None),
      TitleCrew("tt0000032", Some(List("nm0000030")), None),
      TitleCrew("tt0000033", Some(List("nm0000030")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List.empty[(String, Float)]

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "handle directors with missing primary names" in {
    val testName = "Task 3: Handle Missing Director Names"

    val nameBasics = Seq(
      NameBasics("nm0000040", None, None, None, None, None),
      NameBasics("nm0000041", Some("Director Six"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000040", 9.0f, 15000),
      TitleRatings("tt0000041", 8.6f, 20000),
      TitleRatings("tt0000042", 9.1f, 12000),
      TitleRatings("tt0000043", 8.9f, 13000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000040", Some(List("nm0000040")), None),
      TitleCrew("tt0000041", Some(List("nm0000040", "nm0000041")), None),
      TitleCrew("tt0000042", Some(List("nm0000041")), None),
      TitleCrew("tt0000043", Some(List("nm0000041")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Director Six", (8.6f + 9.1f + 8.9f) / 3)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "exclude films with missing ratings or numVotes" in {
    val testName = "Task 3: Missing Ratings or NumVotes"

    val nameBasics = Seq(
      NameBasics("nm0000130", Some("Director Fifteen"), None, None, None, None)
    )

    val titleRatings = Seq(
      TitleRatings("tt0000130", 9.0f, 15000),
      // Simulate missing numVotes by excluding the field
      TitleRatings("tt0000131", 9.0f, 0),
      // Simulate missing averageRating by excluding the field
      TitleRatings("tt0000132", 0.0f, 15000)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000130", Some(List("nm0000130")), None),
      TitleCrew("tt0000131", Some(List("nm0000130")), None),
      TitleCrew("tt0000132", Some(List("nm0000130")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    // Filter out entries with missing or zero values in titleRatings
    val titleRatingsRDD = sc.parallelize(titleRatings).filter(r => r.averageRating > 0.0f && r.numVotes >= 10000)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List.empty[(String, Float)] // Only one valid film; not enough to qualify

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "sort directors alphabetically when multiple directors have the same average rating" in {
    val testName = "Task 3: Tie Sorting with Multiple Directors"

    val nameBasics = Seq(
      NameBasics("nm0000140", Some("Director Alpha"), None, None, None, None),
      NameBasics("nm0000141", Some("Director Beta"), None, None, None, None),
      NameBasics("nm0000142", Some("Director Gamma"), None, None, None, None)
    )

    val titleRatings = Seq(
      // Director Alpha's films
      TitleRatings("tt0000140", 9.0f, 15000),
      TitleRatings("tt0000141", 9.0f, 16000),
      TitleRatings("tt0000142", 9.0f, 17000),
      // Director Beta's films
      TitleRatings("tt0000143", 9.0f, 15000),
      TitleRatings("tt0000144", 9.0f, 16000),
      TitleRatings("tt0000145", 9.0f, 17000),
      // Director Gamma's films
      TitleRatings("tt0000146", 9.0f, 15000),
      TitleRatings("tt0000147", 9.0f, 16000),
      TitleRatings("tt0000148", 9.0f, 17000)
    )

    val titleCrew = Seq(
      // Director Alpha's films
      TitleCrew("tt0000140", Some(List("nm0000140")), None),
      TitleCrew("tt0000141", Some(List("nm0000140")), None),
      TitleCrew("tt0000142", Some(List("nm0000140")), None),
      // Director Beta's films
      TitleCrew("tt0000143", Some(List("nm0000141")), None),
      TitleCrew("tt0000144", Some(List("nm0000141")), None),
      TitleCrew("tt0000145", Some(List("nm0000141")), None),
      // Director Gamma's films
      TitleCrew("tt0000146", Some(List("nm0000142")), None),
      TitleCrew("tt0000147", Some(List("nm0000142")), None),
      TitleCrew("tt0000148", Some(List("nm0000142")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Director Alpha", 9.0f),
      ("Director Beta", 9.0f),
      ("Director Gamma", 9.0f)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD)
      .collect()
      .toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    val passed = result == expected

    printTestStatus(testName, executionTime, passed)

    result shouldEqual expected
  }

  "Task3: Top Directors" should "handle directors with identical names but different IDs appropriately" in {
    val testName = "Task 3: Identical Director Names"

    // Prepare test data
    val nameBasics = Seq(
      NameBasics("nm0000150", Some("Director Smith"), None, None, None, None),
      NameBasics("nm0000151", Some("Director Smith"), None, None, None, None)
    )

    val titleRatings = Seq(
      // Films for the first Director Smith (nm0000150)
      TitleRatings("tt0000150", 9.0f, 15000),
      TitleRatings("tt0000151", 8.9f, 16000),
      TitleRatings("tt0000152", 9.1f, 17000),
      // Films for the second Director Smith (nm0000151)
      TitleRatings("tt0000153", 9.2f, 15000),
      TitleRatings("tt0000154", 9.3f, 16000),
      TitleRatings("tt0000155", 9.4f, 17000)
    )

    val titleCrew = Seq(
      // Films for the first Director Smith
      TitleCrew("tt0000150", Some(List("nm0000150")), None),
      TitleCrew("tt0000151", Some(List("nm0000150")), None),
      TitleCrew("tt0000152", Some(List("nm0000150")), None),
      // Films for the second Director Smith
      TitleCrew("tt0000153", Some(List("nm0000151")), None),
      TitleCrew("tt0000154", Some(List("nm0000151")), None),
      TitleCrew("tt0000155", Some(List("nm0000151")), None)
    )

    val nameBasicsRDD = sc.parallelize(nameBasics)
    val titleRatingsRDD = sc.parallelize(titleRatings)
    val titleCrewRDD = sc.parallelize(titleCrew)

    // Expected averages for each Director Smith
    val avgFirstDirector = (9.0f + 8.9f + 9.1f) / 3
    val avgSecondDirector = (9.2f + 9.3f + 9.4f) / 3

    val expected = List(
      ("Director Smith", avgSecondDirector),
      ("Director Smith", avgFirstDirector)
    ).sortBy { case (name, avg) => (-avg, name) }

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD)
      .collect()
      .toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Since both directors have the same name, ensure both entries are present with correct averages
    val correctLength = result.length == expected.length

    // Check that both average ratings are present for "Director Smith"
    val resultAverages = result.map(_._2).sorted.reverse
    val expectedAverages = expected.map(_._2).sorted.reverse
    val correctAverages = resultAverages == expectedAverages

    val correctSorting = result == expected

    val passed = correctLength && correctAverages && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!passed) {
      if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
      if (!correctAverages) {
        println("Average ratings mismatch. Expected vs Actual:")
        expectedAverages.zipAll(resultAverages, -1.0f, -1.0f).foreach { case (exp, res) =>
          println(f"Expected: $exp%.2f, Got: $res%.2f")
        }
      }
      if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by average rating descending and name ascending.")
      println("Actual Results:")
      result.foreach { case (name, avg) => println(f"($name, $avg%.2f)") }
    }

    result should have length expected.length
    result.map(_._2).sorted.reverse shouldEqual expected.map(_._2).sorted.reverse
    result shouldEqual expected
  }

  "Task4: Prolific Crew Members" should "identify top contributors across time periods and sort alphabetically when counts are the same" in {
    val expected = List(
      ("Stan Lee", 10),
      ("Jack Kirby", 8),
      ("Dustin Ferguson", 7),
      ("Satoshi Tajiri", 7),
      ("William Shakespeare", 7),
      ("Alim Sudio", 6),
      ("Jordan Hill", 6),
      ("Jun'ichi Masuda", 6),
      ("Ken Sugimori", 6),
      ("Marco Romano", 6)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titleBasicsRDD, nameBasicsRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == 10
    val correctContent = result == expected

    // Verify sorting by count descending, then name ascending
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus("Task 4", executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected 10, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    // Original assertions
    result should have length 10
    result should contain theSameElementsInOrderAs expected
  }

  // Test 1: Basic Functionality Test - Verify that the function correctly identifies the top 10 crew members based on unique movie counts across specified periods.

  "Task4: Prolific Crew Members" should "correctly identify top 10 crew members based on unique movie counts across specified periods" in {
    val testName = "Task 4: Basic Functionality Test"

    // Prepare test data
    val titles = Seq(
      // Movies within specified periods
      TitleBasics("tt0000001", Some("movie"), Some("Movie1"), Some("OriginalTitle1"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000002", Some("movie"), Some("Movie2"), Some("OriginalTitle2"), 0, Some(1996), None, None, None),
      TitleBasics("tt0000003", Some("movie"), Some("Movie3"), Some("OriginalTitle3"), 0, Some(2005), None, None, None),
      TitleBasics("tt0000004", Some("movie"), Some("Movie4"), Some("OriginalTitle4"), 0, Some(2006), None, None, None),
      TitleBasics("tt0000005", Some("movie"), Some("Movie5"), Some("OriginalTitle5"), 0, Some(2015), None, None, None),
      TitleBasics("tt0000006", Some("movie"), Some("Movie6"), Some("OriginalTitle6"), 0, Some(2016), None, None, None),
      TitleBasics("tt0000007", Some("movie"), Some("Movie7"), Some("OriginalTitle7"), 0, Some(1997), None, None, None),
      TitleBasics("tt0000008", Some("movie"), Some("Movie8"), Some("OriginalTitle8"), 0, Some(2007), None, None, None),
      TitleBasics("tt0000009", Some("movie"), Some("Movie9"), Some("OriginalTitle9"), 0, Some(2017), None, None, None),
      TitleBasics("tt0000010", Some("movie"), Some("Movie10"), Some("OriginalTitle10"), 0, Some(1998), None, None, None),
      TitleBasics("tt0000011", Some("movie"), Some("Movie11"), Some("OriginalTitle11"), 0, Some(2008), None, None, None),
      TitleBasics("tt0000012", Some("movie"), Some("Movie12"), Some("OriginalTitle12"), 0, Some(2018), None, None, None),
      TitleBasics("tt0000013", Some("movie"), Some("Movie13"), Some("OriginalTitle13"), 0, Some(1999), None, None, None),
      TitleBasics("tt0000014", Some("movie"), Some("Movie14"), Some("OriginalTitle14"), 0, Some(2009), None, None, None),
      TitleBasics("tt0000015", Some("movie"), Some("Movie15"), Some("OriginalTitle15"), 0, Some(2019), None, None, None),
      TitleBasics("tt0000016", Some("movie"), Some("Movie16"), Some("OriginalTitle16"), 0, Some(2000), None, None, None),
      TitleBasics("tt0000017", Some("movie"), Some("Movie17"), Some("OriginalTitle17"), 0, Some(2010), None, None, None),
      TitleBasics("tt0000018", Some("movie"), Some("Movie18"), Some("OriginalTitle18"), 0, Some(2020), None, None, None),
      TitleBasics("tt0000019", Some("movie"), Some("Movie19"), Some("OriginalTitle19"), 0, Some(2000), None, None, None),
      TitleBasics("tt0000020", Some("movie"), Some("Movie20"), Some("OriginalTitle20"), 0, Some(2010), None, None, None),
      // Additional movies for counts
      TitleBasics("tt0000021", Some("movie"), Some("Movie21"), Some("OriginalTitle21"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000022", Some("movie"), Some("Movie22"), Some("OriginalTitle22"), 0, Some(2005), None, None, None),
      TitleBasics("tt0000023", Some("movie"), Some("Movie23"), Some("OriginalTitle23"), 0, Some(2015), None, None, None),
      TitleBasics("tt0000024", Some("movie"), Some("Movie24"), Some("OriginalTitle24"), 0, Some(1996), None, None, None),
      TitleBasics("tt0000025", Some("movie"), Some("Movie25"), Some("OriginalTitle25"), 0, Some(2006), None, None, None),
      TitleBasics("tt0000026", Some("movie"), Some("Movie26"), Some("OriginalTitle26"), 0, Some(2015), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000001", Some("Crew Member One"), None, None, None, None),
      NameBasics("nm0000002", Some("Crew Member Two"), None, None, None, None),
      NameBasics("nm0000003", Some("Crew Member Three"), None, None, None, None),
      NameBasics("nm0000004", Some("Crew Member Four"), None, None, None, None),
      NameBasics("nm0000005", Some("Crew Member Five"), None, None, None, None),
      NameBasics("nm0000006", Some("Crew Member Six"), None, None, None, None),
      NameBasics("nm0000007", Some("Crew Member Seven"), None, None, None, None),
      NameBasics("nm0000008", Some("Crew Member Eight"), None, None, None, None),
      NameBasics("nm0000009", Some("Crew Member Nine"), None, None, None, None),
      NameBasics("nm0000010", Some("Crew Member Ten"), None, None, None, None)
    )

    val titleCrew = Seq(
      // Crew Member One: 5 movies
      TitleCrew("tt0000001", Some(List("nm0000001")), None),
      TitleCrew("tt0000002", Some(List("nm0000001")), None),
      TitleCrew("tt0000021", Some(List("nm0000001")), None),
      TitleCrew("tt0000022", Some(List("nm0000001")), None),
      TitleCrew("tt0000023", Some(List("nm0000001")), None),
      // Crew Member Two: 4 movies
      TitleCrew("tt0000003", Some(List("nm0000002")), None),
      TitleCrew("tt0000004", Some(List("nm0000002")), None),
      TitleCrew("tt0000024", Some(List("nm0000002")), None),
      TitleCrew("tt0000025", Some(List("nm0000002")), None),
      // Crew Member Three: 3 movies
      TitleCrew("tt0000005", Some(List("nm0000003")), None),
      TitleCrew("tt0000006", Some(List("nm0000003")), None),
      TitleCrew("tt0000026", Some(List("nm0000003")), None),
      // Crew Members Four to Ten: 2 movies each
      TitleCrew("tt0000007", Some(List("nm0000004")), None),
      TitleCrew("tt0000008", Some(List("nm0000004")), None),
      TitleCrew("tt0000009", Some(List("nm0000005")), None),
      TitleCrew("tt0000010", Some(List("nm0000005")), None),
      TitleCrew("tt0000011", Some(List("nm0000006")), None),
      TitleCrew("tt0000012", Some(List("nm0000006")), None),
      TitleCrew("tt0000013", Some(List("nm0000007")), None),
      TitleCrew("tt0000014", Some(List("nm0000007")), None),
      TitleCrew("tt0000015", Some(List("nm0000008")), None),
      TitleCrew("tt0000016", Some(List("nm0000008")), None),
      TitleCrew("tt0000017", Some(List("nm0000009")), None),
      TitleCrew("tt0000018", Some(List("nm0000009")), None),
      TitleCrew("tt0000019", Some(List("nm0000010")), None),
      TitleCrew("tt0000020", Some(List("nm0000010")), None)
    )

    // Create RDDs
    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member One", 5),
      ("Crew Member Two", 4),
      ("Crew Member Three", 3),
      ("Crew Member Eight", 2),
      ("Crew Member Five", 2),
      ("Crew Member Four", 2),
      ("Crew Member Nine", 2),
      ("Crew Member Seven", 2),
      ("Crew Member Six", 2),
      ("Crew Member Ten", 2)
    ).sortBy(r => (-r._2, r._1))

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }

  "Task4: Prolific Crew Members" should "exclude movies outside the specified time periods" in {
    val testName = "Task 4: Filtering by Time Periods"

    // Prepare test data
    val titles = Seq(
      // Movies within specified periods
      TitleBasics("tt0000030", Some("movie"), Some("Movie30"), Some("OriginalTitle30"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000031", Some("movie"), Some("Movie31"), Some("OriginalTitle31"), 0, Some(2005), None, None, None),
      TitleBasics("tt0000032", Some("movie"), Some("Movie32"), Some("OriginalTitle32"), 0, Some(2015), None, None, None),
      // Movies outside specified periods
      TitleBasics("tt0000033", Some("movie"), Some("Movie33"), Some("OriginalTitle33"), 0, Some(1994), None, None, None),
      TitleBasics("tt0000034", Some("movie"), Some("Movie34"), Some("OriginalTitle34"), 0, Some(2001), None, None, None),
      TitleBasics("tt0000035", Some("movie"), Some("Movie35"), Some("OriginalTitle35"), 0, Some(2014), None, None, None),
      TitleBasics("tt0000036", Some("movie"), Some("Movie36"), Some("OriginalTitle36"), 0, Some(2021), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000030", Some("Crew Member Thirty"), None, None, None, None)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000030", Some(List("nm0000030")), None),
      TitleCrew("tt0000031", Some(List("nm0000030")), None),
      TitleCrew("tt0000032", Some(List("nm0000030")), None),
      TitleCrew("tt0000033", Some(List("nm0000030")), None),
      TitleCrew("tt0000034", Some(List("nm0000030")), None),
      TitleCrew("tt0000035", Some(List("nm0000030")), None),
      TitleCrew("tt0000036", Some(List("nm0000030")), None)
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member Thirty", 3)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }


  "Task4: Prolific Crew Members" should "exclude titles without a startYear" in {
    val testName = "Task 4: Exclude Titles without startYear"

    // Prepare test data
    val titles = Seq(
      TitleBasics("tt0000040", Some("movie"), Some("Movie40"), Some("OriginalTitle40"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000041", Some("movie"), Some("Movie41"), Some("OriginalTitle41"), 0, None, None, None, None), // Missing startYear
      TitleBasics("tt0000042", Some("movie"), Some("Movie42"), Some("OriginalTitle42"), 0, Some(2005), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000040", Some("Crew Member Forty"), None, None, None, None)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000040", Some(List("nm0000040")), None),
      TitleCrew("tt0000041", Some(List("nm0000040")), None),
      TitleCrew("tt0000042", Some(List("nm0000040")), None)
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member Forty", 2)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }

  "Task4: Prolific Crew Members" should "handle titles with missing directors and writers" in {
    val testName = "Task 4: Handle Missing Directors and Writers"

    // Prepare test data
    val titles = Seq(
      TitleBasics("tt0000050", Some("movie"), Some("Movie50"), Some("OriginalTitle50"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000051", Some("movie"), Some("Movie51"), Some("OriginalTitle51"), 0, Some(2005), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000050", Some("Crew Member Fifty"), None, None, None, None)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000050", None, None), // Missing directors and writers
      TitleCrew("tt0000051", Some(List("nm0000050")), None)
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member Fifty", 1)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }

  "Task4: Prolific Crew Members" should "count each crew member only once per movie even if they have multiple roles" in {
    val testName = "Task 4: Duplicate Crew Members per Movie"

    // Prepare test data
    val titles = Seq(
      TitleBasics("tt0000060", Some("movie"), Some("Movie60"), Some("OriginalTitle60"), 0, Some(1995), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000060", Some("Crew Member Sixty"), None, None, None, None)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000060", Some(List("nm0000060")), Some(List("nm0000060"))) // Same person as director and writer
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member Sixty", 1)
    )

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }

  "Task4: Prolific Crew Members" should "sort the final output by count descending and name ascending" in {
    val testName = "Task 4: Sorting Order Correctness"

    // Prepare test data
    val titles = Seq(
      TitleBasics("tt0000130", Some("movie"), Some("Movie130"), Some("OriginalTitle130"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000131", Some("movie"), Some("Movie131"), Some("OriginalTitle131"), 0, Some(2005), None, None, None),
      TitleBasics("tt0000132", Some("movie"), Some("Movie132"), Some("OriginalTitle132"), 0, Some(2015), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000130", Some("Crew Member ThirteenZero"), None, None, None, None),
      NameBasics("nm0000131", Some("Crew Member ThirteenOne"), None, None, None, None),
      NameBasics("nm0000132", Some("Crew Member ThirteenTwo"), None, None, None, None)
    )

    val titleCrew = Seq(
      // Different counts
      TitleCrew("tt0000130", Some(List("nm0000130")), None), // 1
      TitleCrew("tt0000131", Some(List("nm0000130", "nm0000131")), None), // nm0000130: 2, nm0000131:1
      TitleCrew("tt0000132", Some(List("nm0000130", "nm0000131", "nm0000132")), None) // nm0000130:3, nm0000131:2, nm0000132:1
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Crew Member ThirteenZero", 3),
      ("Crew Member ThirteenOne", 2),
      ("Crew Member ThirteenTwo", 1)
    ).sortBy(r => (-r._2, r._1))

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
      .sortBy(r => (-r._2, r._1))
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Verify results
    val correctLength = result.length == expected.length
    val correctContent = result == expected
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
    if (!correctContent) {
      println("Content mismatch. Expected vs Actual:")
      expected.zip(result).foreach { case (exp, res) =>
        println(s"Expected: $exp, Got: $res")
      }
    }
    if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")

    result should have length expected.length
    result should contain theSameElementsInOrderAs expected
  }

  "Task4: Prolific Crew Members" should "treat crew members with the same name but different IDs as separate individuals" in {
    val testName = "Task 4: Identical Names with Different IDs"

    // Prepare test data
    val titles = Seq(
      TitleBasics("tt0000150", Some("movie"), Some("Movie150"), Some("OriginalTitle150"), 0, Some(1995), None, None, None),
      TitleBasics("tt0000151", Some("movie"), Some("OriginalTitle151"), Some("OriginalTitle151"), 0, Some(2005), None, None, None)
    )

    val names = Seq(
      NameBasics("nm0000150", Some("Alex Johnson"), None, None, None, None),
      NameBasics("nm0000151", Some("Alex Johnson"), None, None, None, None)
    )

    val titleCrew = Seq(
      TitleCrew("tt0000150", Some(List("nm0000150")), None),
      TitleCrew("tt0000151", Some(List("nm0000151")), None)
    )

    val titlesRDD = sc.parallelize(titles)
    val namesRDD = sc.parallelize(names)
    val titleCrewRDD = sc.parallelize(titleCrew)

    val expected = List(
      ("Alex Johnson", 1),
      ("Alex Johnson", 1)
    ).sortBy(r => (-r._2, r._1))

    val startTime = System.currentTimeMillis()
    val result = ImdbSpark.task4(titlesRDD, namesRDD, titleCrewRDD).collect().toList
    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime

    // Since we have duplicate names, verify that there are two entries for "Alex Johnson" with count 1
    val countOfAlexJohnson = result.count { case (name, count) => name == "Alex Johnson" && count == 1 }
    val correctLength = result.length == expected.length
    val correctContent = countOfAlexJohnson == 2
    val correctSorting = result == result.sortBy(r => (-r._2, r._1))

    val passed = correctLength && correctContent && correctSorting
    printTestStatus(testName, executionTime, passed)

    if (!passed) {
      if (!correctLength) println(s"Length mismatch: Expected ${expected.length}, Got ${result.length}")
      if (!correctContent) {
        println("Content mismatch. Expected two entries for 'Alex Johnson' with count 1.")
        println("Actual results:")
        result.foreach(println)
      }
      if (!correctSorting) println(s"Sorting mismatch: Results are not sorted by count descending and name ascending.")
    }

    result should have length expected.length
    countOfAlexJohnson shouldBe 2
  }



}

// SparkContext wrapper remains the same
object SparkContextWrapper {
  import org.apache.spark.{SparkConf, SparkContext}

  private var instance: SparkContext = _

  def getInstance(): SparkContext = {
    if (instance == null || instance.isStopped) {
      val conf = new SparkConf()
        .setAppName("ImdbAnalysisTest")
        .setMaster("local[*]")
        .set("spark.ui.enabled", "false")
      instance = new SparkContext(conf)
    }
    instance
  }

  def stop(): Unit = {
    if (instance != null && !instance.isStopped) {
      instance.stop()
      instance = null
    }
  }
}
