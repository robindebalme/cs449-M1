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

  def kNN_sim_normal(user: Int, k: Int): Array[(Int, Double)] = {
    kNN_map.getOrElse(user,{
      var all_sim  = Array.fill(943)(-1, -1.0)
      for(j <- 0 to 942){
        if(j+1 == user) all_sim(j) = (j+1, -1)
        else all_sim(j) = (j+1, preProcess_Similarity(user, j+1, mapArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim))
      }
      all_sim.sortBy(-_._2).take(k+1)
    })
  }
  
  def all_sim_normal(user: Int, k: Int): Array[Double] = {
      var all_sim  = Array.fill(943)(0.0)
      for(j <- 0 to 942){
        if(j+1 == user) all_sim(j) = (0.0)
        else all_sim(j) = (preProcess_Similarity(user, j+1, mapArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim))
      }
      val k_top_users = all_sim.zipWithIndex.sortBy(-_._1).take(k+1).map(_._2) // index users les plus important
      for(j <- 0 to 942){
        if ( !k_top_users.contains(j) || j==(user-1)) 
          all_sim(j) = 0
      }

      all_sim
  }
  

  //val test_knn = kNN_sim(1, 10)
  //val test_knn_normal = kNN_sim_normal(1, 4)
  //val all_simi = all_sim_normal(1,10)
  //println("kNN USER1 K3 :" + test_knn.mkString(", "))
  //println("kNN normal USER1 K3 :" + test_knn_normal.mkString(", "))
  //println("all simi USER1 K3 :" + all_simi.mkString(", "))
  //println("kNN USER1 K3 :" + test_knn.tail.head)
  //println("kNN USER1 K3 :" + test_knn.tail.tail.head)
  //println("kNN USER1 K3 :" + test_knn.tail.tail.tail.head)


  /*def knn_similarity(u: Int, k: Int) : Array[Double] = {
    val all_sim = Array.fill(943)(0.0)
    var j = 0
    for(j <- 0 to 942){
      all_sim(j) = Similarity(u, j+1)
    }
    val k_top_users = all_sim.zipWithIndex.sortBy(-_._1).take(k+1).map(_._2) // index users les plus important
    for(j <- 0 to 942){
      if ( !k_top_users.contains(j) || j==(u-1)) 
        all_sim(j) = 0
    }
    all_sim
  }

  def knn_anyAvgDev(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val arrFiltered2 = arrFiltered.filter(x => !(x.rating.isNaN)).filter(y => !(y.user.isNaN))

    val all_simU = knn_similarity(u, k)
    if(arrFiltered2.isEmpty) 0 // A REVOIR, C'EST LE CAS D'UN FILM SANS RATING PAR UN USER
    else {
      val haut = arrFiltered2.foldLeft(0.0){(acc, x) =>
        if(all_simU(x.user-1) == 0) acc
        else acc + all_simU(x.user-1)*NormDev(i, x.user, info)
        } // C EST PAS VRAIMENT LA BONNE FORMULE, A CHECK

      val bas = arrFiltered2.foldLeft(0.0){(acc, x) =>
        if(all_simU(x.user-1) == 0) acc
        else acc + all_simU(x.user-1)
        }
    haut/bas
    }
  }*/

  
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
    haut/bas
    }
  }

  def kNN_anyAvgDev_test(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val possible = (arrFiltered.groupBy(x => x.user)).map(x => (x._1, x._2.head.rating))

    val all_simU = kNN_sim(u, k)
    if(arrFiltered.isEmpty) 0
    else {
      val div = all_simU.foldLeft((0.0, 0.0, 0)){(acc, x) => 
        val tmp = possible.getOrElse(x._1, -1.0)
        if (tmp == -1.0 || x._2 == 0) acc
        else 
          (acc._1 + x._2 * dev(tmp, userAvg(x._1, train, alluserAvg, globalAvg)), acc._2 + x._2.abs, acc._3 + 1)
        }
        if(div._2 == 0 || div._3 == 0) 0
        else div._1/div._2
    }
  }
  


  def kNN_anyAvgDev_all_sim(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val possible = (arrFiltered.groupBy(x => x.user)).map(x => (x._1, x._2.head.rating))

    val all_simU = all_sim_normal(u, k)
    if(arrFiltered.isEmpty) 0
    else {
      val div = possible.foldLeft(0.0, 0.0){(acc, x) =>
        if(all_simU(x._1 - 1) == 0) acc
        else (acc._1 + all_simU(x._1 - 1)*dev(x._2, userAvg(x._1, train, alluserAvg, globalAvg)), acc._2 + all_simU(x._1 - 1).abs)
      }
    if(div._2 == 0) 0
    else div._1/div._2
    }
  }
  
  
  
  //val possible_test = (train.filter(x => x.item == 1).groupBy(x => x.user)).map(x => (x._1, x._2.head.rating))
  //println("possible test :" + possible_test.getOrElse(868, 0.0) )
  

  /*def knn_pred(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val useravg = allUserAvg(u-1)
    val anyavgdev = knn_anyAvgDev(i,u,k, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + anyavgdev*scale((useravg+anyavgdev), useravg)

  }*/

  def kNN_pred_(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val useravg = userAvg(u, train, alluserAvg, globalAvg)
    val kNNavgdev = kNN_anyAvgDev(i,u,k, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + kNNavgdev * scale((useravg + kNNavgdev), useravg)

  }

  def kNN_pred_test(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val useravg = userAvg(u, train, alluserAvg, globalAvg)
    val kNNavgdev = kNN_anyAvgDev_test(i,u,k, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + kNNavgdev * scale((useravg + kNNavgdev), useravg)

  }

  def kNN_pred_test_all(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val useravg = userAvg(u, train, alluserAvg, globalAvg)
    val kNNavgdev = kNN_anyAvgDev_all_sim(i,u,k, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + kNNavgdev * scale((useravg + kNNavgdev), useravg)

  }




  def mae(k: Int, test: Array[Rating], train: Array[Rating], prediction_method: String): Double = {
    if (prediction_method == "GlobalAvg")
      test.foldLeft(0.0){(acc, x) => acc + (globalAvg - x.rating).abs}/test.length
    else if (prediction_method == "knn")
     test.foldLeft(0.0){(acc, x) => acc + (kNN_pred_(x.user, x.item, k,  train) - x.rating).abs}/test.length
    else if (prediction_method == "knn_test")
     test.foldLeft(0.0){(acc, x) => acc + (kNN_pred_test(x.user, x.item, k,  train) - x.rating).abs}/test.length
    else 0
  }

  val mae_k10 = mae(10, test, train, "knn_test_all")

  println("mae k10 :" + mae_k10)


  //println("Pred U1 I1 K10 :" + mae(943, test, train, "knn"))
  /*println("Pred U1 I1 K10 :" + knn_pred(1,1, 30, train))
  println("Pred U1 I1 K10 :" + knn_pred(1,1, 50, train))
  println("Pred U1 I1 K10 :" + knn_pred(1,1, 100, train))*/
  //println("recommendation for new user : " + recommendation(945, 5, 2, train))
  //println("MAE K10 :" + mae(10, test, train, "knn"))

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
