package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

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

  def sim_anyAvgDev(i: Int, u: Int, info: Array[Rating]): Double = {
    val arrFiltered = info.filter(x => x.item == i)
    val arrFiltered2 = arrFiltered.filter(x => !(x.rating.isNaN)).filter(y => !(y.user.isNaN))
    if(arrFiltered2.isEmpty) 0 // A REVOIR, C'EST LE CAS D'UN FILM SANS RATING PAR UN USER
    else {
      val haut = arrFiltered2.foldLeft(0.0){(acc, x) =>
        val sim = Similarity(u, x.user)
        if(sim == 0) acc
        else acc + sim*NormDev(i, x.user, info)
        } // C EST PAS VRAIMENT LA BONNE FORMULE
      val bas = arrFiltered2.foldLeft(0.0){(acc, x) =>
        val sim = Similarity(u, x.user)
        if(sim == 0) acc
        else acc + Similarity(u, x.user)
        }
      haut/bas
    }
      
  }
  
  def PredRat(u: Int, i: Int, info: Array[Rating]): Double = {

    val useravg = allUserAvg(u-1)
    val anyavgdev = sim_anyAvgDev(i,u, info)

    if (info.filter(x => x.item == i).isEmpty) useravg
    else if (info.filter(x => x.user == u).isEmpty) globalAvg 
    else useravg + anyavgdev*scale((useravg+anyavgdev), useravg)
  }
  
  
  def preProc(i: Int,u: Int): Double = {
    val arrFiltered = train.filter(x => x.user == u ).filter(x => !(x.rating.isNaN)).filter(y => !(y.item.isNaN))
    if(arrFiltered.isEmpty) 0
    else NormDev(i, u, train)/sqrt(arrFiltered.foldLeft(0.0){(acc, x) =>
                                      val normDevU = NormDev(x.item, u, train)
                                      if(normDevU == 0) acc
                                      else acc + normDevU*normDevU
                                      })
  }
  def preProc_Similarity(u: Int, v: Int): Double = { // DONNE PAS DES BONS RESULTATS
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
          preProc(x, u)*preProc(x, v)
          }
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
          "1.PredUser1Item1" -> ujson.Num(PredRat(1,1, train)), // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> ujson.Num(0.0)         // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> ujson.Num(0.0), // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> ujson.Num(0.0),  // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> ujson.Num(0.0) // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> ujson.Num(0.0), // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> ujson.Num(0.0),  // Prediction item 1 for user 1 (jaccard)
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
