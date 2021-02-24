import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class find_visitors_pipeline_test extends FunSuite {
  val sc = new SparkContext("local[*]" , "CodingChallengeTest")
  val spark = SparkSession.builder().appName("CodingChallengeTest").getOrCreate()
  val class_object_test = new find_visitors_pipeline_utilities()

  test("apply_custom_schema() applied to a dataframe should output desired dataframe"){
    // test input file named test_input and expected output in test_output_1
    val actual_df = class_object_test.apply_custom_schema(spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "false")
      .csv("src/test/resources/test_input"))

    val expected_df = spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/test/resources/test_output_1")

    // assert dataframe counts
    assert(actual_df.count() == expected_df.count())
    // assert dataframe similarity by ensuring that no outstanding rows exist in any
    assert(actual_df.except(expected_df).count() == expected_df.except(actual_df).count())
    // assert outstanding non-similar rows are not existing with count property
    assert(actual_df.except(expected_df).count() == 0)
  }

  test("find_top_visitors() should find most recent top 5 visitors in dataframe for each day") {
    // most recent top 5 visitors input file named test_input_2 and expected output file in test_output_2
    val actual_df = class_object_test.find_top_visitors((spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/test/resources/test_input_2")), 5)

    val expected_df = spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/test/resources/test_output_2")

    // assert dataframe counts
    assert(actual_df.count() == expected_df.count())
    // assert dataframe similarity by ensuring that no outstanding rows exist in any
    assert(actual_df.except(expected_df).count() == expected_df.except(actual_df).count())
    // assert outstanding non-similar rows are not existing with count property
    assert(actual_df.except(expected_df).count() == 0)

  }

  test("find_top_visitors() should find least recent top 3 visitors in dataframe for each day") {
    // most recent top 5 visitors input file named test_input_2 and expected output file in test_output_3
    val actual_df = class_object_test.find_top_visitors((spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/test/resources/test_input_2")), 3, true)

    val expected_df = spark.read
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/test/resources/test_output_3")

    // assert dataframe counts
    assert(actual_df.count() == expected_df.count())
    // assert dataframe similarity by ensuring that no outstanding rows exist in any
    assert(actual_df.except(expected_df).count() == expected_df.except(actual_df).count())
    // assert outstanding non-similar rows are not existing with count property
    assert(actual_df.except(expected_df).count() == 0)

  }
}
