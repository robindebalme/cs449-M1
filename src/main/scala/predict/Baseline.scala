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

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

<<<<<<< HEAD
  //////////////////////////////////
  ////// CE QUE MOI J'AJOUTE //////
  ////////////////////////////////

  ////B1////

  val globalAvg = train.foldLeft(0.0)((acc, x) => acc + x.rating)/train.length 
  val user1len = train.filter(y => y.user == 1).length
  val user1len2 = train.foldLeft(0)((acc, x) => if (x.user ==1) acc+1 else acc)
  val len = train.length

  
  val user1Avg = train.foldLeft(0.0){(acc,x) => 
    if(x.user == 1) acc + x.rating 
    else acc
  }/train.filter(y => y.user == 1).length

  val item1Avg = train.foldLeft(0.0){(acc,x) => 
    if(x.item == 1) acc + x.rating 
    else acc
  }/train.filter(y => y.item == 1).length

  def scale(x: Double, rAvg: Double): Double = {
    if (x>rAvg) 5.0-rAvg
    else if (x < rAvg) rAvg - 1.0
    else if (x == rAvg) 1.0
    else 0 // ESSAYER DE PRINT UNE ERREUR
    
  }

  //// CALCULER AVERAGES POUR CHAQUE INDEX ET MISE DANS UN TABLEAU ////
  def AllUserAvg(info: Array[Rating]): Array[Double] = {
    val array = Array.fill(943)(0.0)
    var j = 0
    for(i <- 0 to 942){
      array(i) = UserAvg(i+1, info)
    }
    array
  }

  val allUserAvg = AllUserAvg(train)

  def AllItemAvg(info: Array[Rating]): Array[Double] = {
    val array = Array.fill(1682)(0.0)
    var j = 0
    for(j <- 0 to 1681){
      array(j) = ItemAvg(j+1, info)
    } 
    array
  }
  
  val allItemAvg = AllItemAvg(train)

  //// FIN CALCUL DES AVERAGES TABLEAU////

  def UserAvg(u: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.user == u)
    if(arrFiltered.isEmpty) globalAvg
    else {
      info.foldLeft(0.0){(acc,x) => 
      if(x.user == u) acc + x.rating 
      else acc
      }/info.filter(y => y.user == u).length
    }
  }

  def ItemAvg(i: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    if (arrFiltered.isEmpty) globalAvg
    else {
      info.foldLeft(0.0){(acc,x) => 
      if(x.item == i) acc + x.rating 
      else acc
      }/info.filter(y => y.item == i).length
    }
  }

  def NormDev(i: Int, u: Int, info: Array[Rating]): Double = {
    val r_u_i = info.filter(x => x.user == u).filter(y => y.item == i)(0).rating
    (r_u_i - allUserAvg(u-1))/(scale(r_u_i, allUserAvg(u-1)))
  }



  def AnyAvgDev(i: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val arrFiltered2 = arrFiltered.filter(x => !(x.rating.isNaN)).filter(y => !(y.user.isNaN))
    if(arrFiltered2.isEmpty) 0 // A REVOIR, C'EST LE CAS D'UN FILM SANS RATING PAR UN USER
    else arrFiltered2.foldLeft(0.0){(acc, x) =>
      acc + NormDev(x.item, x.user, info)/(arrFiltered2.length)
      }
  }

  //EVERY AnyAvgDev DANS UN TABLEAU

  def AllAnyAvgDev(info: Array[Rating]): Array[Double] = {
    val array = Array.fill(1682)(0.0)
    var i = 0
    for(i <- 0 to 1681){
      array(i) = AnyAvgDev(i, info)
    }
    array
  }

  val allAnyAvgDev = AllAnyAvgDev(train)
  println(allAnyAvgDev.filter(x => !(x.isNaN)).foldLeft(0)((acc, y) => acc + 1))

  def PredRat(u: Int, i: Int, info: Array[Rating]): Double = {

    val useravg = allUserAvg(u-1)
    val anyavgdev = allAnyAvgDev(i-1)

    if (info.filter(x => x.item == i).isEmpty) allUserAvg(u-1)
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + anyavgdev*scale((useravg+anyavgdev), useravg)
  }
  

  ////B2////

  def MAE(test: Array[Rating], train: Array[Rating], prediction_method: String): Double = {
    if (prediction_method == "GlobalAvg")
      test.foldLeft(0.0){(acc, x) => acc + (globalAvg - x.rating).abs}/test.length
    else if (prediction_method == "UserAvg")
      test.foldLeft(0.0){(acc, x) => acc + (allUserAvg(x.user-1) - x.rating).abs}/test.length
    else if (prediction_method == "ItemAvg") 
      test.foldLeft(0.0){(acc, x) => acc + (allItemAvg(x.item-1) - x.rating).abs}/test.length
    else if (prediction_method == "BaselineAvg")
     test.foldLeft(0.0){(acc, x) => acc + (PredRat(x.user, x.item, train) - x.rating).abs}/test.length
    else 0
  }

  /////////////////////////////////
  //////// J'AI ARRETER LA ///////
  ///////////////////////////////


=======
  val globalAvg = train.foldLeft(0.0){(acc : Double, arr: Rating) => acc + arr.rating} / train.length

  val user1Avg = train.filter(arr => arr.user == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == 1).length

  val item1Avg = train.filter(arr => arr.item == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.item == 1).length

  def anyUserAvg(user : Int, arr : Array[Rating]) : Double = {
    val arrFiltered = arr.filter(arr => arr.user == user)
    if (arrFiltered.isEmpty)
      globalAvg
    else
     arrFiltered.foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == user).length
  }

  def anyItemAvg(item : Int, arr : Array[Rating]) : Double =  {
    val arrFiltered = arr.filter(arr => arr.item == item)
    if (arrFiltered.isEmpty)
      globalAvg
    else
      arrFiltered.foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.item == item).length
    }

  def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  def anyAvgDev(item : Int, arr : Array[Rating]): Double = {
    arr.filter(l => l.item == item).foldLeft(0.0){(acc, elem) => 
      val userAvg = anyUserAvg(elem.user, arr)
      (elem.rating - userAvg) / scale(elem.rating, userAvg) + acc} / arr.filter(l => l.item == item).length
  }

  def predicted(user: Int, item : Int, arr : Array[Rating]): Double = {
    val userAvg = anyUserAvg(user, arr)
    if (arr.filter(l => l.item == item).isEmpty)
      userAvg
    else {
      val avgDev = anyAvgDev(item, arr)
      userAvg + avgDev * scale((userAvg + avgDev), userAvg)
    }
  }

  def fullpred(train : Array[Rating]): Array[Double] = {
    train.map(elem => predicted(elem.user, elem.item, train))
  }

  def abs(x : Double): Double = {
    if (x <= 0.0)
      -x 
    else x
  }

  def mae(train : Array[Rating], test : Array[Rating], version : String): Double = version match {
    case "GlobalAvg" => 
      val globalAvg = train.foldLeft(0.0){(acc : Double, arr: Rating) => acc + arr.rating} / train.length
      test.foldLeft(0.0){(acc, elem ) =>  abs(globalAvg - elem.rating)} / test.length

    case _ => -1
  }
>>>>>>> origin
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
          "1.GlobalAvg" -> ujson.Num(globalAvg), // Datatype of answer: Double
<<<<<<< HEAD
          "2.User1Avg" -> ujson.Num(UserAvg(1, train)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(ItemAvg(1, train)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(AnyAvgDev(1, train)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(PredRat(1, 1, train)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(MAE(test, train, "GlobalAvg")), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(MAE(test, train, "UserAvg")),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(MAE(test, train, "ItemAvg")),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(MAE(test, train, "BaselineAvg"))   // Datatype of answer: Double
=======
          "2.User1Avg" -> ujson.Num(anyUserAvg(1, train)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(anyItemAvg(1, train)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(anyAvgDev(1, train)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(predicted(1, 1, train)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(mae(train, test, "GlobalAvg")), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(0.0),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(0.0),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(0.0)   // Datatype of answer: Double
>>>>>>> origin
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
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
