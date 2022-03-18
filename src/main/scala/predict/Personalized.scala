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


  globalAvg =  mean_(train.map(_.rating))


  // Nouveau pour cet exo

  def common_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Int] = {
    //commonItemMap.getOrElse((arrU.head.user, arrV.head.user), {
    val itemIn2 = arrV.map(_.item).distinct
    val common_item_Ur = arrU.filter(elem => itemIn2.contains(elem.item)).map(_.item)
    //commonItemMap += (((arrU.head.user, arrV.head.user), common_item_Ur))
    common_item_Ur  
    //})
  }

  def union_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Int] = {
    //commonItemMap.getOrElse((arrU.head.user, arrV.head.user), {
    val itemIn2 = arrV.map(_.item).distinct
    val uncommon_item_Ur = arrU.filter(elem => !itemIn2.contains(elem.item)).map(_.item)
    val union_item = uncommon_item_Ur ++ itemIn2
    //commonItemMap += (((arrU.head.user, arrV.head.user), common_item_Ur))
    union_item 
    //})
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
        val normDevU = dev((arrFiltered_u.filter(_.item == x)).head.rating, uAvg)
        val normDevV = dev((arrFiltered_v.filter(_.item == x)).head.rating, vAvg)
        acc + normDevU * normDevV
        }

      val bottom = (sqrt(arrFiltered_u.foldLeft(0.0){(acc, elem) =>
        val normDevU = dev(elem.rating, uAvg)
        acc + normDevU * normDevU
        }) * sqrt(arrFiltered_v.foldLeft(0.0){(acc, elem2) =>
        val normDevV = dev(elem2.rating, vAvg)
        acc + normDevV * normDevV
        }))

      tmp = top / bottom
      cosineSim += (((u,v), tmp))
      tmp
      }
    })
  }

  def jaccardSimilarity(u: Int, v: Int, filteredArrUsers: Map[Int, Array[Rating]]): Double = {
    jaccardSim.getOrElse((u, v), {

    val arrFiltered_u = filteredArrUsers.getOrElse(u, train.filter(x => x.user == u ))
    val arrFiltered_v = filteredArrUsers.getOrElse(v, train.filter(x => x.user == v ))

    val item_commun = common_item(arrFiltered_u , arrFiltered_v )

    var tmp = 0.0
    if(item_commun.isEmpty) {
      jaccardSim += (((u,v), tmp))
      jaccardSim += (((v,u), tmp))
      tmp
    }

    else {
      val uAvg = userAvg(u, train, alluserAvg, globalAvg)
      val vAvg = userAvg(v, train, alluserAvg, globalAvg)

      val top = item_commun.foldLeft(0.0){(acc, x) =>
        val normDevU = dev((arrFiltered_u.filter(_.item == x)).head.rating, uAvg)
        val normDevV = dev((arrFiltered_v.filter(_.item == x)).head.rating, vAvg)
        acc + normDevU * normDevV
        }
      
      val bottom = (arrFiltered_u.foldLeft(0.0){(acc, elem) =>
        val normDevU = dev(elem.rating, uAvg)
        acc + normDevU
        } + arrFiltered_v.foldLeft(0.0){(acc, elem2) =>
        val normDevV = dev(elem2.rating, vAvg)
        acc + normDevV * normDevV
        } - top)
  

      
      tmp = top / bottom
      jaccardSim += (((u,v), tmp))
      jaccardSim += (((v,u), tmp))


      tmp
      }
    })
  }
  
  println("Cosine similarity :" + cosineSimilarity(1, 2))
  //println("Jaccard similarity :" + jaccardSimilarity(1, 2, filteredArrUsers))


  //println("union_item :" + union_item(train.filter(_.user == 1 ) , train.filter(_.user == 2 )).head)
  //println("commun_item :" + common_item(train.filter(_.user == 1 ) , train.filter(_.user == 2 )).head)



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
  

  def jaccard_avgSimilarity(i: Int, u: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]]): Double = {
    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val top = arrFiltered.foldLeft((0.0)){(acc, x) =>
        val sim = jaccardSimilarity(u, x.user, filteredArrUsers)
        (acc + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))))
        }
      val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
        val sim = jaccardSimilarity(u, x.user, filteredArrUsers)
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
        val top = dev(rating_u_i, userAvg(u, train, alluserAvg, globalAvg)) 
        val bottom = arrFiltered.foldLeft(0.0){(acc, x) =>
          val normDevU = dev(x.rating, userAvg(u, train, alluserAvg, globalAvg))
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
    val arrFiltered_u = filteredArrUsers.getOrElse(u, train.filter(x => x.user == u ))
    val arrFiltered_v = filteredArrUsers.getOrElse(v, train.filter(x => x.user == v ))
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
        val simavgdev = avgSimilarity(item ,user, train, mapArrUsers)
        if (simavgdev == 0) {
          useravg
        }
        else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
      }
    }
  }
  
  def jaccard_predictedPersonalized(user: Int, item : Int): Double = {
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
        val simavgdev = jaccard_avgSimilarity(item ,user, train, mapArrUsers)
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

  mapArrUsers = filteredArrAllUsers(train)

  def predictorCosine(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => predictedPersonalized(user, item)
  }

  def predictorJaccard(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => jaccard_predictedPersonalized(user, item)
  }

  val measurements1 = (1 to conf.num_measurements()).map(x => timingInMs(() => {

    alluserAvg = mutable.Map()
    allitemAvg = mutable.Map()
    allitemDev = mutable.Map()
    cosineSim = mutable.Map()
    jaccardSim = mutable.Map()
    mapArrUsers = filteredArrAllUsers(train)


    globalAvg =  mean_(train.map(_.rating))
    //Thread.sleep(1000) // Do everything here from train and test
    //42        // Output answer as last value
     mae(test, train, predictorJaccard)

  }))

  val timingsGlobalAvg = measurements1.map(t => t._2)

  println("Jaccard MAE :" + mae(test, train, predictorJaccard))

  /*val measurements1 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val a = predictorCosine(train)(12,3)
    val b = predictorCosine(train)(3,9)
    val c = predictorCosine(train)(8,2)
    val d = predictorCosine(train)(4,40)
    predictorCosine(train)(1,1)
    

  }))

  val timingsGlobalAvg = measurements1.map(t => t._2)*/


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
          "1.PredUser1Item1" -> ujson.Num(predictorBaseline(train)(1, 1))/*, // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> ujson.Num(mae(test, train, predictorBaseline)) */        // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> ujson.Num(0.0), // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> ujson.Num(0.0),  // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> ujson.Num(0.0) // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> ujson.Num(0.0), // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> ujson.Num(mae(test, train, predictorCosine)),  // Prediction item 1 for user 1 (jaccard)
          "3.JaccardPersonalizedMAE" -> ujson.Num(mean(timingsGlobalAvg)) // MAE when using jaccard similarity
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
