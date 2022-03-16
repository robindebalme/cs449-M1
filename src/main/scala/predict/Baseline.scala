package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()

  val sizeOfTrain = train.length
  val sizeOfTest = test.length
  val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
  val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

  var alluserAvg = Map(0 -> 0.0)

  var allitemAvg = Map(0 -> 0.0)

  var singleDev = Map((0.0, 0.0) -> 0.0)

  var allitemDev = Map(0 -> 0.0)

  var pred = Map((0, 0) -> 0.0)



<<<<<<< HEAD
  def AnyAvgDev(i: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val arrFiltered2 = arrFiltered.filter(x => !(x.rating.isNaN)).filter(y => !(y.user.isNaN))
    if(arrFiltered2.isEmpty) 0 // A REVOIR, C'EST LE CAS D'UN FILM SANS RATING PAR UN USER
    else arrFiltered2.foldLeft(0.0){(acc, x) =>
      acc + NormDev(i, x.user, info)/(arrFiltered2.length)
=======
  val globalAvg =  train.foldLeft(0.0)((acc, elem) => acc + elem.rating)/ sizeOfTrain

  def userAvg(user : Int): Double = {
    if (alluserAvg.get(user) != None) 
      alluserAvg(user)
    else 
      {
      val filtered = train.filter(elem => elem.user == user)
      var tmp = 0.0
      if (filtered.isEmpty)
        tmp = globalAvg
      else
        tmp = filtered.foldLeft(0.0)((acc, elem) => acc + elem.rating) / filtered.length

      alluserAvg = alluserAvg.updated(user, tmp)
      tmp
>>>>>>> fbebf150bd413f7121032fdf78a155f9ef8c22ac
      }
  }

  def itemAvg(item : Int): Double = {
    if (allitemAvg.get(item) != None) 
      allitemAvg(item)
    else 
      {
      val filtered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (filtered.isEmpty)
        tmp = globalAvg
      else
        tmp = filtered.foldLeft(0.0)((acc, elem) => acc + elem.rating) / filtered.length

      allitemAvg = allitemAvg.updated(item, tmp)
      tmp
      }
  }

   def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  def dev(r: Double, useravg : Double): Double = {
    if (singleDev.get(r, useravg) != None)
      singleDev(r, useravg)
    else
      {
      val tmp = (r - useravg) / scale(r, useravg)
      singleDev = singleDev.updated((r, useravg), tmp)
      tmp
    }
  }

  def avgDev(item : Int): Double = {
    if (allitemDev.get(item) != None) 
      allitemDev(item)
    else {
      var tmp = train.filter(l => l.item == item)
      var tmp2 = tmp.foldLeft(0.0){(acc, elem) =>
        acc + dev(elem.rating, userAvg(elem.user))} / tmp.length
      allitemDev =  allitemDev.updated(item, tmp2)
      tmp2
    }
  }
 
  def predicted(user: Int, item : Int): Double = {
    if (pred.get(user, item) != None)
      pred(user, item)
    else{
      val useravg = userAvg(user)
      if (useravg == globalAvg) {
        pred = pred.updated((user, item), globalAvg)
        globalAvg
      }
      else
        if (train.filter(l => l.item == item).isEmpty) {
          pred = pred.updated((user, item), useravg)
          useravg
        }
        else {
          val avgdev = avgDev(item)
          if (avgdev == 0) {
            pred = pred.updated((user, item), useravg)
            useravg
          }
          else
            pred = pred.updated((user, item), useravg + avgdev * scale((useravg + avgdev), useravg))
            useravg + avgdev * scale((useravg + avgdev), useravg)
        }
    }
  }
  
<<<<<<< HEAD
  println("predrat cas 1 :" + allUserAvg(0))
  println("predrat cas 2 :" + allAnyAvgDev(17-1))
  //println("predrat cas 3 :" + (scale((allUserAvg(0)+AnyAvgDev(1, train)), allUserAvg(0))))
=======
 
  //for(
  //  u <- 1 to maxUser
  //) yield userAvg(u)
>>>>>>> fbebf150bd413f7121032fdf78a155f9ef8c22ac

  //for(
  //  i <- 1 to maxItem
  //) yield itemAvg(i)


  //for(
  //  u <- 1 to maxUser;
  //  i <- 1 to maxItem
  //) yield predicted(u, i)

  def mae(test: Array[Rating], train: Array[Rating], prediction_method: String): Double = {
    if (prediction_method == "GlobalAvg")
      test.foldLeft(0.0){(acc, x) => acc + (globalAvg - x.rating).abs}/sizeOfTest
    else if (prediction_method == "UserAvg")
      test.foldLeft(0.0){(acc, x) => acc + (userAvg(x.user) - x.rating).abs}/sizeOfTest
    else if (prediction_method == "ItemAvg") 
      test.foldLeft(0.0){(acc, x) => acc + (itemAvg(x.item) - x.rating).abs}/sizeOfTest
    else if (prediction_method == "BaselineAvg")
     test.foldLeft(0.0){(acc, x) => acc + (predicted(x.user, x.item) - x.rating).abs}/sizeOfTest
    else 0
  }



  val measurements1 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val sizeOfTrain = train.length
    val sizeOfTest = test.length
    val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
    val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

    var alluserAvg = Map(0 -> 0.0)
    var allitemAvg = Map(0 -> 0.0)
    var singleDev = Map((0.0, 0.0) -> 0.0)
    var allitemDev = Map(0 -> 0.0)
    var pred = Map((0, 0) -> 0.0)

    val globalAvg =  train.foldLeft(0.0)((acc, elem) => acc + elem.rating)/ sizeOfTrain
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
    mae(test, train, "GlobalAvg")

  }))

  val timingsGlobalAvg = measurements1.map(t => t._2)
  
  val measurements2 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val sizeOfTrain = train.length
    val sizeOfTest = test.length
    val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
    val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

    var alluserAvg = Map(0 -> 0.0)
    var allitemAvg = Map(0 -> 0.0)
    var singleDev = Map((0.0, 0.0) -> 0.0)
    var allitemDev = Map(0 -> 0.0)
    var pred = Map((0, 0) -> 0.0)

    val globalAvg =  train.foldLeft(0.0)((acc, elem) => acc + elem.rating)/ sizeOfTrain
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
    mae(test, train, "UserAvg")
    
  }))

  val timingsUserAvg = measurements2.map(t => t._2)

  val measurements3 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val sizeOfTrain = train.length
    val sizeOfTest = test.length
    val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
    val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

    var alluserAvg = Map(0 -> 0.0)
    var allitemAvg = Map(0 -> 0.0)
    var singleDev = Map((0.0, 0.0) -> 0.0)
    var allitemDev = Map(0 -> 0.0)
    var pred = Map((0, 0) -> 0.0)

    val globalAvg =  train.foldLeft(0.0)((acc, elem) => acc + elem.rating)/ sizeOfTrain
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
    mae(test, train, "ItemAvg")
    
  }))

  val timingsItemAvg = measurements3.map(t => t._2)

  val measurements4 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val sizeOfTrain = train.length
    val sizeOfTest = test.length
    val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
    val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

    var alluserAvg = Map(0 -> 0.0)
    var allitemAvg = Map(0 -> 0.0)
    var singleDev = Map((0.0, 0.0) -> 0.0)
    var allitemDev = Map(0 -> 0.0)
    var pred = Map((0, 0) -> 0.0)

    val globalAvg =  train.foldLeft(0.0)((acc, elem) => acc + elem.rating)/ sizeOfTrain
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
    mae(test, train, "BaselineAvg")
    
  }))

  val timingsBaselineAvg = measurements4.map(t => t._2)

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(maxUser), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(userAvg(1)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(itemAvg(1)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(avgDev(1)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(predicted(1, 1)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(mae(test, train, "GlobalAvg")), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(mae(test, train, "UserAvg")),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(mae(test, train, "ItemAvg")),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(mae(test, train, "BaselineAvg"))   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsGlobalAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsGlobalAvg)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsUserAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsUserAvg)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsItemAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsItemAvg)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsBaselineAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsBaselineAvg)) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
