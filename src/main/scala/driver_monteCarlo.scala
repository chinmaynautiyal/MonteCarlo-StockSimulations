
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Column, DataFrame, Row, RowFactory, SparkSession, functions}
import Numeric.Implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.commons.math3.distribution.NormalDistribution

import collection.JavaConverters._
import java.io.File

import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object driver_monteCarlo extends LazyLogging {

  def main(args: Array[String]): Unit = {

    //Run many simulations in parallel


    //creating spark context
    val configuration = new SparkConf().setAppName("Monte Carlo Financial Simulations").setMaster("local[2]")
    val sc = new SparkContext(configuration)

    //import data into RDDs
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate
    logger.info("Reading msft data into dataframe")

    //get portfolio data from config files

    val conf = ConfigFactory.load()

    //list of stocks you are considering
    val stockList = conf.getStringList("stocks").asScala
    val weightsOfStocks: List[Double] = conf.getStringList("weights").asScala.map(x => x.toDouble).toList


    val stockDataDir: String = args(0).toString
    val numtrials = conf.getString("numTrials")


    //load all data into for all stocks in stockList
    val dfList: List[DataFrame] = stockList.map { i =>
      spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load(stockDataDir + i + ".csv")
        .drop("Open", "High", "Low", "Adj Close", "Volume")
        .withColumnRenamed("Close", i)
      //withColumn("Symbol", functions.lit(i) )
    }.toList




    //merge dataframes into one dataframe


    val mainDF = dfList match {
      case head :: tail => tail.foldLeft(head) { (df1, df2) => df1.join(df2, "Date") }
      case Nil => spark.emptyDataFrame
    }

    //println(mainDF.show())


    //test filtering multiple columns
    val stockListtest: List[String] = List("AAPL", "TSLA")


    //how many days in mainDF?
    val totalDays = 2 //mainDF.count()


    //here we do the simulation
    val numUniverses: Int = conf.getString("numUniverses").toInt //parallelization
    val timePeriod = conf.getString(("timePeriod")).toInt

    val startSimDate = 1 //totalDays/2


    var investment = conf.getString("initialInvestment").toInt


    (startSimDate to totalDays).foreach { i =>
      var daysPassed = 0
      while ((daysPassed < totalDays) && (investment > 0)) {
        //we are at start date of the simulation


        //pick some random stock, query that from the dataset, simulate its price changes for the given time period

        val randomNumber = scala.util.Random.nextInt(stockList.length)
        val chosenStock: String = stockList(randomNumber)

        //extract prices for that stack from the dataframe
        val listOfPrices = mainDF.select(chosenStock).rdd.map(r => java.lang.Double.parseDouble(r(0).toString)).collect().toList

        //run the simulation and get average results from simulating the change in prices
        val resultSim: Array[Double]= (sc.parallelize(1 to numUniverses)).map(i => simpleSimulation(listOfPrices, startSimDate, startSimDate+timePeriod )).collect()
        val maxMean = resultSim.max
        val threshold: Double = maxMean/listOfPrices(daysPassed)


        //check if current price meets our condition for investing

        if(threshold*100 > 110) {
          //if threshold condition is met buy more of that stock
          investment = investment - (investment/4) //invest 25% in stock

        }
        else if ( threshold * 100 < 50 )
          {
            //sell
            investment = investment + investment/10
          }
        val investmentList = new ListBuffer[Int]()
        investmentList += investment




        daysPassed = daysPassed + 1


      }
    }


    logger.info("Monte Carlo Simulations complete")

  }

  def simpleSimulation(priceList: List[Double], timePeriod: Int, start: Int): Double = {
    val sample: List[Double]= samplePrice(priceList)
    val slicedSample = sample.slice(start, start+ timePeriod)

    //get mean of list and return, for decision process
    val average  = mean(slicedSample)
    average



  }



  /**
   *
   * @param stocks     list of stocks which might be bought
   * @param weights    parameters describing distribution of investment
   * @param time       parameter over which the simulation generates statistics
   * @param data       main RDD containing all data
   * @param totalMoney money at this particular instance in the simulation
   * @return avg return for simulating through 'time' days
   */
  def runSimulation(stocks: List[String], weights: List[Double], time: String, data: sql.DataFrame, totalMoney: Int): Unit = {

    //this is where you randomly generate the changes in stock value and return the max and min values for stocks chosen
    //Step 1: randomly choose stock

    //randomly shuffle the stock and pick a random number of stocks

    logger.info("Randomly choosing possible stock purchase")
    val randomNumber = 2 //scala.util.Random.nextInt(stocks.length)
    val shuffledList = scala.util.Random.shuffle(stocks)

    val chosenStocks = shuffledList.take(randomNumber)
    //add Date column with stocks such that it is filtered

    //make new list buffer with "Date"
    val stockBuffer = chosenStocks.to[ListBuffer]
    stockBuffer += "Date"


    //extract chosen stock data from dataframe


    //test filtering data and return to driver object
    val filteredData = filterStocks(data, stockBuffer.toList)




  }


  /**
   *
   *
   * @param priceList receives price list for stock
   * @return the list with new sampled prices
   */
  def samplePrice(priceList: List[Double]): List[Double] = {

    //extrapolate the prices and randomly get the next price according to formula
    //get standard deviation and mean for the List and generate new prices

    //calculate new prices from the distribution
    val sd = stdDev(priceList)
    val va = variance(priceList)
    val me = mean(priceList)

    val distribution = new NormalDistribution(me, sd)
    priceList.map(i => distribution.sample())


    priceList
  }

    /**
     *
     * @param allstockData dataframe containing all the data which must be filtered
     * @param stockList    list of chosen stocks for simulation, which should be filtered from the data
     * @return filtered data for stocks which we want for the given computation
     */
    def filterStocks(allstockData: DataFrame, stockList: List[String]): DataFrame = {

      logger.info("Filtering data based on given stock")
      //append date to stockList, essentially stock list is to filtered out of the data

      //allstockData.select(allstockData.columns.filter(colName => stockList.contains(colName)).map(colName => new Column(colName)): _*)

      allstockData.select(stockList.head, stockList.tail: _*)
    }

    def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

    def variance[T: Numeric](xs: Iterable[T]): Double = {
      val avg = mean(xs)

      xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
    }

    def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))



}
