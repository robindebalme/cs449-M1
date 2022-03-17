package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable
import scala.math._
import shared.predictions._


class PersonalizedConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Personalized extends App {
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
  
  // Compute here

  //////////////////////////////////
  ////// CE QUE MOI J'AJOUTE //////
  ////////////////////////////////

  val sizeOfTrain = train.length
  val sizeOfTest = test.length
  val maxUser = List(train.map(elem => elem.user).max, test.map(elem => elem.user).max).max
  val maxItem = List(train.map(elem => elem.item).max, test.map(elem => elem.item).max).max

  val alluserAvg : mutable.Map[Int, Double] = mutable.Map()
  val allitemAvg : mutable.Map[Int, Double] = mutable.Map()
  val allitemDev : mutable.Map[Int, Double] = mutable.Map()
  val singleDev : mutable.Map[(Double, Double),Double] = mutable.Map()
  val cosineSim : mutable.Map[(Int, Int),Double] = mutable.Map()

  val globalAvg =  mean_(train.map(_.rating))

  /*def scale(x: Double, rAvg: Double): Double = {
    if (x>rAvg) 5.0-rAvg
    else if (x < rAvg) rAvg - 1.0
    else if (x == rAvg) 1.0
    else 0 // ESSAYER DE PRINT UNE ERREUR
    
  }*/

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

  def common_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Rating] = {
    val itemIn2 = arrV.map(_.item).distinct
    val common_item_Ur = arrU.filter(elem => itemIn2.contains(elem.item))
    common_item_Ur   
  }

  def cosineSimilarity(u: Int, v: Int): Double = {
    cosineSim.getOrElse((u, v), {
    val arrFiltered_u = train.filter(_.user == u )
    val arrFiltered_v = train.filter(_.user == v )

    val item_commun = common_item(arrFiltered_u , arrFiltered_v )

    var tmp = 0.0
    if(item_commun.isEmpty) {
      cosineSim += (((u,v), tmp))
      tmp
    }
    else {
      val uAvg = userAvg(u, train, alluserAvg, globalAvg)
      val vAvg = userAvg(v, train, alluserAvg, globalAvg)

      val top = item_commun.foldLeft(0.0){(acc, x) =>
        val normDevU = dev(x.rating, uAvg, singleDev)
        val normDevV = dev((arrFiltered_v.filter(_.item == x.item)).apply(0).rating, vAvg, singleDev)
        acc + normDevU * normDevV
        }

      val bottom = (sqrt(arrFiltered_u.foldLeft(0.0){(acc, elem) =>
        val normDevU = dev(elem.rating, uAvg, singleDev)
        acc + normDevU * normDevU
        }) * sqrt(arrFiltered_v.foldLeft(0.0){(acc, elem2) =>
        val normDevV = dev(elem2.rating, vAvg, singleDev)
        acc + normDevV * normDevV
        }))

      tmp = top / bottom
      cosineSim += (((u,v), tmp))
      tmp
      }
    })
  }


  def avgSimilarity(i: Int, u: Int, train: Array[Rating]): Double = {
    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val top = arrFiltered.foldLeft((0.0)){(acc, x) =>
        val sim = preProcess_Similarity(u, x.user)
        (acc + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg), singleDev)))
        }
      val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
        val sim = preProcess_Similarity(u, x.user)
        acc + sim.abs
        }
      top / bottom
    }  
  }
  
  def predictedPersonalized(user: Int, item : Int): Double = {
    val useravg = userAvg(user, train, alluserAvg, globalAvg)
    if (useravg == globalAvg) {
      globalAvg
    }
    else{
      val itemavg = itemAvg(item, train, allitemAvg, globalAvg)
      if (itemavg == globalAvg) {
        useravg
      }
      else {
        val simavgdev = avgSimilarity(item ,user, train)
        if (simavgdev == 0) {
          useravg
        }
        else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
      }
    }
  }
  
  
  def preProcess(i: Int,u: Int): Double = {
    val arrFiltered = train.filter(x => x.user == u )
    if(arrFiltered.isEmpty) 0
    else {
      val rating_u_i = arrFiltered.filter(_.item == i).apply(0).rating
      val top = dev(rating_u_i, userAvg(u, train, alluserAvg, globalAvg), singleDev) 
      val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
        val normDevU = dev(x.rating, userAvg(u, train, alluserAvg, globalAvg), singleDev)
        acc + normDevU*normDevU
      }
      top / sqrt(bottom)
    }
  }
  
  def preProcess_Similarity(u: Int, v: Int): Double = { 
    val arrFiltered_u = train.filter(x => x.user == u )
    val arrFiltered_v = train.filter(x => x.user == v )

    val item_commun = common_item(arrFiltered_u, arrFiltered_v).map(_.item).distinct

    if(item_commun.isEmpty) 0
    else
        item_commun.foldLeft(0.0){(acc, x) => acc + (preProcess(x, u) * preProcess(x, v))
    }
  }



  def predictorCosine(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => predictedPersonalized(user, item)
  }

  def MAE(test: Array[Rating], train: Array[Rating], prediction_method: String): Double = {
    if (prediction_method == "Sim1")
      test.foldLeft(0.0){(acc, x) => acc + (globalAvg - x.rating).abs}/test.length
    else if (prediction_method == "Cosine")
      test.foldLeft(0.0){(acc, x) => acc + (allUserAvg(x.user-1) - x.rating).abs}/test.length
    else if (prediction_method == "Jacard") 
      test.foldLeft(0.0){(acc, x) => acc + (allItemAvg(x.item-1) - x.rating).abs}/test.length
    else 0
  }


  val measurements1 = (1 to conf.num_measurements()).map(x => timingInMs(() => {

    val alluserAvg : mutable.Map[Int, Double] = mutable.Map()
    val allitemAvg : mutable.Map[Int, Double] = mutable.Map()
    val allitemDev : mutable.Map[Int, Double] = mutable.Map()
    val singleDev : mutable.Map[(Double, Double),Double] = mutable.Map()


    val globalAvg =  mean_(train.map(_.rating))
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
    mae(test, train, predictorCosine)

  }))

  val timingsGlobalAvg = measurements1.map(t => t._2)

  /////////////////////////////////
  //////// J'AI ARRETER LA ///////
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
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "P.1" -> ujson.Obj(
          "1.PredUser1Item1" -> ujson.Num(predictedPersonalized(1, 1)), // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> ujson.Num(predictedBaseline(1, 1, train, singleDev, allitemDev, globalAvg, alluserAvg, allitemAvg))         // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> ujson.Num(cosineSimilarity(1, 8)), // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> ujson.Num(avgSimilarity(1, 1, train)),  // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> ujson.Num(mae(test, train, predictorCosine)) // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> ujson.Num(preProcess_Similarity(1, 2)), // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> ujson.Num(cosineSimilarity(1, 2)),  // Prediction item 1 for user 1 (jaccard)
          "3.JaccardPersonalizedMAE" -> ujson.Num(0.0) // MAE when using jaccard similarity
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
