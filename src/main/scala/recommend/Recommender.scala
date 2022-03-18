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


  // Nouveau pour cet exo

  def common_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Int] = {
    //commonItemMap.getOrElse((arrU.head.user, arrV.head.user), {
    val itemIn2 = arrV.map(_.item).distinct
    val common_item_Ur = arrU.filter(elem => itemIn2.contains(elem.item)).map(_.item)
    //commonItemMap += (((arrU.head.user, arrV.head.user), common_item_Ur))
    common_item_Ur  
    //})
  }

  def avgSimilarity(i: Int, u: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]]): Double = {
    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val top = arrFiltered.foldLeft((0.0)){(acc, x) =>
        val sim = preProcess_Similarity(u, x.user, filteredArrUsers)
        (acc + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))))
        }
      val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
        val sim = preProcess_Similarity(u, x.user, filteredArrUsers)
        acc + sim.abs
        }
      top / bottom
    }  
  }
  
  
  def preProcess(i: Int,u: Int, arrFiltered : Array[Rating]): Double = {
    preProcessSim.getOrElse((u, i),{
      var tmp = 0.0
      if (arrFiltered.isEmpty){
        preProcessSim += (((u,i), tmp))
        tmp
      }
      else {
        val rating_u_i = arrFiltered.filter(_.item == i).apply(0).rating
        val top = dev(rating_u_i, userAvg(u, data, alluserAvg, globalAvg)) 
        val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
          val normDevU = dev(x.rating, userAvg(u, data, alluserAvg, globalAvg))
          acc + normDevU * normDevU
        }
        tmp = top / sqrt(bottom)
        preProcessSim += (((u,i), tmp))
        tmp
      }
    })
  }
  
  def preProcess_Similarity(u: Int, v: Int, filteredArrUsers: Map[Int, Array[Rating]]): Double = { 
    cosineSim.getOrElse((u, v), {
    val arrFiltered_u = filteredArrUsers.getOrElse(u, data.filter(x => x.user == u ))
    val arrFiltered_v = filteredArrUsers.getOrElse(v, data.filter(x => x.user == v ))
    //val arrFiltered_u = train.filter(x => x.user == u )
    //val arrFiltered_v = train.filter(x => x.user == v )

    val item_commun = common_item(arrFiltered_u, arrFiltered_v)
    var tmp = 0.0
    if(item_commun.isEmpty){
      cosineSim += (((u,v), tmp))
      cosineSim += (((v,u), tmp))
      tmp
    }
    else
        tmp = item_commun.foldLeft(0.0){(acc, x) => acc + (preProcess(x, u, arrFiltered_u) * preProcess(x, v, arrFiltered_v))}
        cosineSim += (((u,v), tmp))
        cosineSim += (((v,u), tmp))
        tmp
    })
  }

  def predictedPersonalized(user: Int, item : Int): Double = {
    val useravg = userAvg(user, data, alluserAvg, globalAvg)
    if (useravg == globalAvg) {
      globalAvg
    }
    else{
      val itemavg = itemAvg(item, data, allitemAvg, globalAvg)
      if (itemavg == globalAvg) {
        useravg
      }
      else {
        val simavgdev = avgSimilarity(item ,user, data, mapArrUsers)
        if (simavgdev == 0) {
          useravg
        }
        else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
      }
    }
  }
  

  def filteredArrAllUsers(train : Array[Rating]): Map[Int, Array[Rating]] =  {
    train.groupBy(elem => elem.user)
  }

  mapArrUsers = filteredArrAllUsers(data)

  def predictorCosine(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => predictedPersonalized(user, item)
  }


  def recommendation(u: Int, n: Int, info: Array[Rating]) : Array[Int] = {

      val arrFiltered = info.filter(x => x.user == u).filter(x => !(x.rating.isNaN))
      val init = Array.fill(1682)(-1.0)
      for(i <- arrFiltered){
        init(i.item-1) = i.rating
      }
      for (j <- 0 to 1681){
        if(init(j) == -1) init(j) = predictedPersonalized(j+1, u) //FAUT COSINE KNN
      }
    init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice list

  }

  def test_recommendation_rating(u: Int, n: Int, info: Array[Rating]) : Array[Double] = {

      val arrFiltered = info.filter(x => x.user == u).filter(x => !(x.rating.isNaN))
      val init = Array.fill(1682)(-1.0)
      for(i <- arrFiltered){
        init(i.item-1) = i.rating
      }
      for (j <- 0 to 1681){
        if(init(j) == -1) init(j) = predictedPersonalized(j+1, u)
      }
    val out = init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice list
    out.map(x => init(x-1))

  }

  
  //val rec = recommendation(900, 3, data)
  val data_augmented = Array.concat(data, personal)
  val rec_test = test_recommendation_rating(900, 3, data_augmented)
  println("Recommendation USER 1 : 1): " + rec_test.head)
  println("Recommendation USER 1 : 2): " + rec_test.tail.head)
  println("Recommendation USER 1 : 3) " + rec_test.tail.tail.head)
  //println("Recommendation USER 1 : 4) " + rec_test.tail.tail.tail.head)

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
