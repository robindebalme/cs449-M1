package recommend

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math._ // PAS LA ORIGINALEMENT
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val json = opt[String]()
  verify()
}

object Recommender extends App {
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
  println("Loading data from: " + conf.data()) 
  val data = load(spark, conf.data(), conf.separator()).collect()
  assert(data.length == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  val personal = personalFile.map(l => {
      val cols = l.split(",").map(_.trim)
      if (cols(0) == "id") 
        Rating(944,0,0.0)
      else 
        if (cols.length < 3) 
          Rating(944, cols(0).toInt, 0.0)
        else
          Rating(944, cols(0).toInt, cols(2).toDouble)
  }).filter(r => r.rating != 0).collect()
  val movieNames = personalFile.map(l => {
      val cols = l.split(",").map(_.trim)
      if (cols(0) == "id") (0, "header")
      else (cols(0).toInt, cols(1).toString)
  }).collect().toMap



  ////////////////////////////////////////
  ////////////////////////////////////////

  globalAvg =  mean_(data.map(_.rating))
  val data_augmented = Array.concat(data, personal)
  
  // Nouveau pour cet exo

  def common_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Int] = {
    //commonItemMap.getOrElse((arrU.head.user, arrV.head.user), {
    val itemIn2 = arrV.map(_.item).distinct
    val common_item_Ur = arrU.filter(elem => itemIn2.contains(elem.item)).map(_.item)
    //commonItemMap += (((arrU.head.user, arrV.head.user), common_item_Ur))
    common_item_Ur  
    //})
  }
  

  ////////////////////////////////////////
  ///////// NEW STRAT DIMANCHE //////////
 ///////////////////////////////////////
 val train = data

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


  def predictor_knn(train : Array[Rating], k: Int): (Int, Int) => Double = {
    (user, item) => predictedPersonalized_knn(user, item, k, train)
  }
  

  def mae_test(test: Array[Rating], train: Array[Rating], k: Int): Double = {
    mean_(test.map(elem => (predictor_knn(train, k)(elem.user, elem.item) - elem.rating).abs))
  }

  /////////////////////////////////
  //////////FIN DIMANCHE /////////
  ///////////////////////////////

  ///////////////////////////////
  def recommendation(u: Int, n: Int, k: Int, info: Array[Rating]) : Array[Int] = {

      val arrFiltered = info.filter(x => x.user == u).filter(x => !(x.rating.isNaN))
      val init = Array.fill(1682)(-1.0)
      for(i <- arrFiltered){
        init(i.item-1) = i.rating
      }
      for (j <- 0 to 1681){
        if(init(j) == -1) init(j) = predictedPersonalized_knn(j+1, u, k, info)
      }
    init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice array

  }

  def test_recommendation_rating(u: Int, n: Int, k: Int, info: Array[Rating]) : Array[Double] = {

      val arrFiltered = info.filter(x => x.user == u).filter(x => !(x.rating.isNaN))
      val init = Array.fill(1682)(-1.0)
      for(i <- arrFiltered){
        init(i.item-1) = i.rating
      }
      for (j <- 0 to 1681){
        if(init(j) == -1) init(j) = predictedPersonalized_knn(j+1, u, k, info)
      }
    val out = init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice array
    out.map(x => init(x-1))

  }

  
  //val rec = recommendation(900, 3, data)
  val rec_test = test_recommendation_rating(900, 3, 10, data_augmented)
  println("Recommendation USER 1 : 1): " + rec_test.mkString(", "))

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
          "data" -> conf.data(),
          "personal" -> conf.personal()
        ),
        "R.1" -> ujson.Obj(
          "PredUser1Item1" -> ujson.Num(0.0) // Prediction for user 1 of item 1
        ),
          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-3 recommendations that have the smallest
          // movie identifier.

        "R.2" -> List((254, 0.0), (338, 0.0), (615, 0.0)).map(x => ujson.Arr(x._1, movieNames(x._1), x._2))
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
