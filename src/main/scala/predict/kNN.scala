package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math._
import shared.predictions._


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

  val globalAvg = train.foldLeft(0.0)((acc, x) => acc + x.rating)/train.length

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
    val arrFiltered = info.filter(x => x.user == u).filter(y => y.item == i)
    if(arrFiltered.isEmpty) 0
    else (arrFiltered(0).rating - allUserAvg(u-1))/(scale(arrFiltered(0).rating, allUserAvg(u-1)))    
  }

  // Nouveau pour cet exo

  def Similarity(u: Int, v: Int): Double = {
    val arrFiltered_u = train.filter(x => x.user == u ).filter(x => !(x.rating.isNaN)).filter(y => !(y.item.isNaN))
    val arrFiltered_v = train.filter(x => x.user == v ).filter(x => !(x.rating.isNaN)).filter(y => !(y.item.isNaN))

    val item_commun = arrFiltered_u.foldLeft[List[Int]](List()){(accU, x) =>
                        val a = arrFiltered_v.foldLeft[Int](-1){(accV, y) =>
                            if(x.item == y.item) x.item
                            else accV
                            }
                        if(a == (-1)) accU
                        else a :: accU
                        }

    if(item_commun.isEmpty) 0
    else
        item_commun.foldLeft(0.0){(acc, x) =>
          val normDevU = NormDev(x, u, train)
          val normDevV = NormDev(x, v, train)
          acc + normDevU*normDevV
          }/(sqrt(arrFiltered_u.foldLeft(0.0){(acc, x) =>
                val normDevU = NormDev(x.item, u, train)
                acc + normDevU*normDevU
                })*sqrt(arrFiltered_v.foldLeft(0.0){(acc, y) =>
                  val normDevV = NormDev(y.item, v, train)
                  acc + normDevV*normDevV
                  }))
  }

  def knn_similarity(u: Int, k: Int) : Array[Double] = {
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
  }

  def knn_pred(i: Int, u: Int, k: Int, info: Array[Rating]): Double = {
    val useravg = allUserAvg(u-1)
    val anyavgdev = knn_anyAvgDev(i,u,k, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + anyavgdev*scale((useravg+anyavgdev), useravg)

  }

  def mae(k: Int, test: Array[Rating], train: Array[Rating], prediction_method: String): Double = {
    if (prediction_method == "GlobalAvg")
      test.foldLeft(0.0){(acc, x) => acc + (globalAvg - x.rating).abs}/test.length
    else if (prediction_method == "UserAvg")
      test.foldLeft(0.0){(acc, x) => acc + (allUserAvg(x.user-1) - x.rating).abs}/test.length
    else if (prediction_method == "ItemAvg") 
      test.foldLeft(0.0){(acc, x) => acc + (allItemAvg(x.item-1) - x.rating).abs}/test.length
    else if (prediction_method == "knn")
     test.foldLeft(0.0){(acc, x) => acc + (knn_pred(x.user, x.item, k,  train) - x.rating).abs}/test.length
    else 0
  }

  
  def recommendation(u: Int, n: Int, k: Int, info: Array[Rating]) : Array[Int] = {

    if(u > 943){ /*
        val init = Array.fill(1682)(0.0)
        for (j <- 0 to 1681){
          init(j) = knn_pred(j+1, u, k, info)
        }
        init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice list
        */
        Array.fill(1682)(0)
    }
    else {
      val arrFiltered = info.filter(x => x.user == u).filter(x => !(x.rating.isNaN))
      val init = Array.fill(1682)(-1.0)
      for(i <- arrFiltered){
        init(i.item-1) = i.rating
      }
      for (j <- 0 to 1681){
        if(init(j) == -1) init(j) = knn_pred(j+1, u, k, info)
      }
      init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice list
    }
  }
  

  println("Pred U1 I1 K10 :" + knn_pred(1,1, 10, train))
  //println("Pred U1 I1 K10 :" + knn_pred(1,1, 30, train))
  //println("Pred U1 I1 K10 :" + knn_pred(1,1, 50, train))
  //println("Pred U1 I1 K10 :" + knn_pred(1,1, 100, train))
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
