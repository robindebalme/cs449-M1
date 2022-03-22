package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math._
import shared.predictions._
import scala.collection.mutable


class kNNConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object kNN extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new PersonalizedConf(args) 
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()


  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

//////
// Modif NEW
//////

  globalAvg =  mean_(train.map(_.rating))
  mapArrUsers = filteredArrAllUsers(train)
  

  ////////////////////////////////////////
  ///////// NEW STRAT DIMANCHE //////////
 ///////////////////////////////////////

def all_similarities(user: Int, k: Int): Array[Double] = {
      var all_sim  = Array.fill(943)(0.0)
      for(j <- 0 to 942){
        if(j+1 == user) all_sim(j) = (0.0)
        else all_sim(j) = (preProcess_Similarity(user, j+1, mapArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim))
      }
      val k_top_users = all_sim.zipWithIndex.sortBy(-_._1).take(k).map(_._2) // index users les plus important
      for(j <- 0 to 942){
        if ( !k_top_users.contains(j) || j==(user-1)) 
          all_sim(j) = 0
      }

      all_sim
  }

  //println("All similarities :" + all_similarities(1, 10).mkString(", "))


  def avgSimilarity_knn(i: Int, u: Int, k: Int, train: Array[Rating]): Double = {

    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val all_sim = all_similarities(u, k)
      val res = arrFiltered.foldLeft((0.0, 0.0)){(acc, x) =>
        val sim = all_sim(x.user - 1)
        (acc._1 + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))), acc._2 + sim.abs)
        }
      if(res._2 == 0) 0
      else res._1 / res._2
    }  
  }

  println("Avg similarity I1 U1 K10 :"  + avgSimilarity_knn(1, 1, 10, train))

  def predictedPersonalized_knn(user: Int, item : Int, k: Int, train: Array[Rating]): Double = {
    
    val useravg = userAvg(user, train, alluserAvg, globalAvg)

    if (useravg == globalAvg) globalAvg
    else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
      val simavgdev = avgSimilarity_knn(item ,user, k, train)
      if (simavgdev == 0) useravg
      else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
    }
  }

  println("Pred I2 U1 K10 :"  + predictedPersonalized_knn(1, 2, 10, train))

  def predictor_knn(train : Array[Rating], k: Int): (Int, Int) => Double = {
    (user, item) => predictedPersonalized_knn(user, item, k, train)
  }
  

  def mae_test(test: Array[Rating], train: Array[Rating], k: Int): Double = {
    mean_(test.map(elem => (predictor_knn(train, k)(elem.user, elem.item) - elem.rating).abs))
  }

  println("mae K10 :"  + mae_test(test, train, 10))

  /////////////////////////////////
  //////////FIN DIMANCHE /////////
  ///////////////////////////////


   ////////////////////////////////
  /////AUTRE FACON (-stockage)////
  ///////////////////////////////

 def kNN_sim(user: Int, k: Int): Array[(Int, Double)] = {
    kNN_map.getOrElse(user,{
    val tmp = train.map(_.user).distinct
    var acc = Array.fill(k)(-1, -1.0)
    val res = tmp.foldLeft(acc){(acc, x) => {
      val sim = preProcess_Similarity(user, x, mapArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim)

      val min = acc.zipWithIndex.foldLeft((0, 100.0), -1){(acc2, y) => {
        if (acc2._1._2 < y._1._2)
          acc2
        else
          (y._1, y._2)
      }
    }
      if (min._1._2 > sim || x == user)
        acc
      else
        acc(min._2) = (x, sim)
        acc
        }
      }
    kNN_map += ((user, res))
    res
    })
  }

  def kNN_anyAvgDev(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val possible = (arrFiltered.groupBy(x => x.user)).map(x => (x._1, x._2.head.rating))

    val all_simU = kNN_sim(u, k)
    if(arrFiltered.isEmpty) 0
    else {
      val haut = all_simU.foldLeft(0.0){(acc, x) => 
        val tmp = possible.getOrElse(x._1, -1.0)
        if (tmp == -1.0 || x._2 == 0) acc
        else acc + x._2 * dev(tmp, userAvg(x._1, train, alluserAvg, globalAvg))
        } 

      val bas = all_simU.foldLeft(0.0){(acc, x) =>
        val tmp = possible.getOrElse(x._1, -1.0)
        if (tmp == -1.0) acc
        else acc + x._2.abs
        } 
    if(bas == 0) 0
    else haut/bas
    
    }
  }

def predictedPersonalized_knn_autre(user: Int, item : Int, k: Int, train: Array[Rating]): Double = {
    
    val useravg = userAvg(user, train, alluserAvg, globalAvg)

    if (useravg == globalAvg) globalAvg
    else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
      val simavgdev = kNN_anyAvgDev(item ,user, k, train)
      if (simavgdev == 0) useravg
      else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
    }
  }

  def predictor_knn_autre(train : Array[Rating], k: Int): (Int, Int) => Double = {
    (user, item) => predictedPersonalized_knn_autre(user, item, k, train)
  }
  

  def mae_test_autre(test: Array[Rating], train: Array[Rating], k: Int): Double = {
    mean_(test.map(elem => (predictor_knn_autre(train, k)(elem.user, elem.item) - elem.rating).abs))
  }

  //println("mae K10 :"  + mae_test_autre(test, train, 10))

  ////////////////////////////////
  //////////FIN DE LA FACON //////
  ///////////////////////////////

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
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Measurements" -> conf.num_measurements()
        ),
        "N.1" -> ujson.Obj(
          "1.k10u1v1" -> ujson.Num(0.0), // Similarity between user 1 and user 1 (k=10)
          "2.k10u1v864" -> ujson.Num(0.0), // Similarity between user 1 and user 864 (k=10)
          "3.k10u1v886" -> ujson.Num(0.0), // Similarity between user 1 and user 886 (k=10)
          "4.PredUser1Item1" -> ujson.Num(0.0) // Prediction of item 1 for user 1 (k=10)
        ),
        "N.2" -> ujson.Obj(
          "1.kNN-Mae" -> List(10,30,50,100,200,300,400,800,943).map(k => 
              List(
                k,
                0.0 // Compute MAE
              )
          ).toList
        ),
        "N.3" -> ujson.Obj(
          "1.kNN" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)),
            "stddev (ms)" -> ujson.Num(std(timings))
          )
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
