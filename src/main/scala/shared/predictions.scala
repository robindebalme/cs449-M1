package shared

import scala.collection.mutable
import scala.math._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import io.netty.util.internal.EmptyArrays


package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0
  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def load(spark : org.apache.spark.sql.SparkSession,  path : String, sep : String) : org.apache.spark.rdd.RDD[Rating] = {
       val file = spark.sparkContext.textFile(path)
       return file
         .map(l => {
           val cols = l.split(sep).map(_.trim)
           toInt(cols(0)) match {
             case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
             case None => None
           }
       })
         .filter({ case Some(_) => true 
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }



  var alluserAvg: mutable.Map[Int, Double] = mutable.Map()
  var allitemAvg: mutable.Map[Int, Double] = mutable.Map()
  var allitemDev: mutable.Map[Int, Double] = mutable.Map()

  var cosineSim: mutable.Map[(Int, Int),Double] = mutable.Map()
  var preProcessSim: mutable.Map[(Int, Int),Double] = mutable.Map()
  var commonItemMap: mutable.Map[(Int, Int),Array[Int]] = mutable.Map()
  var jaccardSim: mutable.Map[(Int, Int),Double] = mutable.Map()
  var kNN_map: mutable.Map[Int, Array[(Int, Double)]] = mutable.Map()
  
  var mapArrUsers: Map[Int, Array[Rating]] =  Map()

  var globalAvg = 0.0

  /* 
  Mean function that compute the mean of an array of double. Using only one
  foldleft enables to compute the mean faster.
  */
  def mean_(arr : Array[Double]): Double = {
    val tmp = arr.foldLeft((0.0, 0))((acc, elem) => (acc._1 + elem, acc._2 + 1))
    tmp._1 / tmp._2
  }

  /*
  Compute the average of ratings of a given array of rating.
  */
  def computeGlobalAvg(train : Array[Rating]): Double = {
    mean_(train.map(_.rating))
  }

  /*
  Compute the average of ratings of a given user and store the result dynamically in a Map for future use.
  */
  def userAvg(user : Int, train : Array[Rating], alluserAvg : mutable.Map[Int, Double], globalAvg : Double): Double = {
    alluserAvg.getOrElse(user, { //Return the average if already computed, otherwise compute it.
      val filtered = train.filter(elem => elem.user == user)
      var tmp = 0.0
      if (filtered.isEmpty) tmp = globalAvg
      else
        tmp = mean_(filtered.map(_.rating))

      alluserAvg += ((user, tmp)) //Update of the Map 
      tmp
    })
  }

  /*
  Compute the average of ratings of a given item and store the result dynamically in a Map for future use.
  */
  def itemAvg(item : Int, train : Array[Rating], allitemAvg : mutable.Map[Int, Double], globalAvg : Double): Double = {
    allitemAvg.getOrElse(item, { //Return the average if already computed, otherwise compute it.
      val filtered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (filtered.isEmpty)
        tmp = globalAvg
      else
        tmp = mean_(filtered.map(_.rating))

      allitemAvg += ((item, tmp)) //Update of the Map
      tmp
    })
  }

  //Scale fonction.
  def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  //Normalized deviation fonction for a given rating and a given user average.
  def dev(r: Double, useravg : Double): Double = {
    (r - useravg) / scale(r, useravg)
  }

  /*
  Compute the average deviation for a given item and store the result dynamically in a Map for future use.
  */
  def itemAvgDev(item : Int, train : Array[Rating], allitemDev : mutable.Map[Int, Double], globalAvg : Double, 
  alluserAvg : mutable.Map[Int, Double]): Double = {
    allitemDev.getOrElse(item,  {
      val tmp = train.filter(_.item == item)
      var tmp2 = 0.0
      if (tmp.isEmpty)
        tmp2 = 0.0
      else
        // mean of all the deviations of the users who rated item i
        tmp2 = mean_(tmp.map(elem => dev(elem.rating, userAvg(elem.user, train, alluserAvg, globalAvg))))
      allitemDev +=  ((item, tmp2)) //Update of the Map
      tmp2
    })
  }

  /*
  Predict rating given by a user on a given item.
  */
  def predictedBaseline(user: Int, item : Int, train : Array[Rating], allitemDev : mutable.Map[Int, Double], globalAvg : Double, 
  alluserAvg : mutable.Map[Int, Double], allitemAvg : mutable.Map[Int, Double]): Double = {
    val useravg = userAvg(user, train, alluserAvg, globalAvg)
    if (useravg == globalAvg) globalAvg
    else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
        val avgdev = itemAvgDev(item, train, allitemDev, globalAvg, alluserAvg)
        if (avgdev == 0) useravg
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
    }
  }

  ////
  // Use for Personalized
  ////

  /*
  Return the common item of two array of ratings.
  */
  def common_item(arrU : Array[Rating], arrV : Array[Rating]): Array[Int] = {
    val itemIn2 = arrV.map(_.item).distinct
    val common_item_Ur = arrU.filter(elem => itemIn2.contains(elem.item)).map(_.item)
    common_item_Ur
  }

  /*
  Return a Map of all the rating given by each users.
  */
  def filteredArrAllUsers(train : Array[Rating]): Map[Int, Array[Rating]] =  {
    train.groupBy(elem => elem.user)
  }

  /*
  Return cosine similarity between 2 users (slow implementation)
  */
  def cosineSimilarity_withfullformula(u: Int, v: Int, train : Array[Rating], alluserAvg : mutable.Map[Int, Double], 
  cosineSim : mutable.Map[(Int, Int),Double], globalAvg : Double): Double = {
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

  /*
  Return the Jaccard Similarity between two given users.
  */
  def jaccardSimilarity(u: Int, v: Int, filteredArrUsers: Map[Int, Array[Rating]], train: Array[Rating], 
  alluserAvg : mutable.Map[Int, Double], globalAvg : Double, jaccardSim : mutable.Map[(Int, Int),Double]): Double = {
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
        acc + 1
        }
      
      val bottom = (arrFiltered_u.foldLeft(0.0){(acc, elem) =>
        acc + 1
        } + arrFiltered_v.foldLeft(0.0){(acc, elem2) =>
        acc + 1
        } - top)
      
      tmp = top / bottom
      jaccardSim += (((u,v), tmp))
      jaccardSim += (((v,u), tmp))
      tmp
      }
    })
  }

  /*
  Preprocess computation to compute Cosine Sim.
  */
  def preProcess(i: Int,u: Int, arrFiltered : Array[Rating], train : Array[Rating], globalAvg : Double, 
  alluserAvg : mutable.Map[Int, Double], preProcessSim : mutable.Map[(Int, Int),Double]): Double = {
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

  /*
  Compute Cosine Similarity between two given users (fast implementation)
  */
  def preProcess_Similarity(u: Int, v: Int, filteredArrUsers: Map[Int, Array[Rating]], cosineSim: mutable.Map[(Int, Int),Double], 
  train: Array[Rating], globalAvg: Double, alluserAvg: mutable.Map[Int, Double], preProcessSim: mutable.Map[(Int, Int),Double]): Double = { 
    cosineSim.getOrElse((u, v), {
    val arrFiltered_u = filteredArrUsers.getOrElse(u, train.filter(x => x.user == u ))
    val arrFiltered_v = filteredArrUsers.getOrElse(v, train.filter(x => x.user == v ))

    val item_commun = common_item(arrFiltered_u, arrFiltered_v)
    var tmp = 0.0
    if(item_commun.isEmpty){
      cosineSim += (((u,v), tmp))
      cosineSim += (((v,u), tmp))
      tmp
    }
    else
        tmp = item_commun.foldLeft(0.0){(acc, item) => acc + (preProcess(item, u, arrFiltered_u, train, globalAvg, 
        alluserAvg, preProcessSim) * preProcess(item, v, arrFiltered_v, train, globalAvg, alluserAvg, preProcessSim))}
        cosineSim += (((u,v), tmp))
        cosineSim += (((v,u), tmp))
        tmp
    })
  }

  /*
  Compute Cosine Similarity between two given users (fast implementation)
  */
  def avgSimilarity(i: Int, u: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]], 
  cosineSim: mutable.Map[(Int, Int),Double], globalAvg: Double, alluserAvg: mutable.Map[Int, Double], preProcessSim: mutable.Map[(Int, Int),Double]): Double = {
    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val res = arrFiltered.foldLeft((0.0, 0.0)){(acc, x) =>
        val sim = preProcess_Similarity(u, x.user, filteredArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim)
        (acc._1 + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))), acc._2 + sim.abs)
        }
      res._1 / res._2
    }  
  }

  def jaccard_avgSimilarity(i: Int, u: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]], globalAvg: Double, 
  alluserAvg: mutable.Map[Int, Double], jaccardSim : mutable.Map[(Int, Int),Double]): Double = {
    val arrFiltered = train.filter(_.item == i)
    if(arrFiltered.isEmpty) 0
    else {
      val res = arrFiltered.foldLeft((0.0, 0.0)){(acc, x) =>
        val sim = jaccardSimilarity(u, x.user, filteredArrUsers, train, alluserAvg, globalAvg, jaccardSim)
        (acc._1 + (sim * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))), acc._2 + sim.abs)
        }
      res._1 / res._2
    }  
  }


  def best_similarities_knn(user: Int, k: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]], globalAvg: Double, 
  alluserAvg: mutable.Map[Int, Double], preProcessSim: mutable.Map[(Int, Int),Double], cosineSim: mutable.Map[(Int, Int),Double], maxUser: Int): Map[Int, Double] /*Array[Int]*/ = {
    
      val allSim = (1 to maxUser).toArray.map(x => (x, preProcess_Similarity(user, x, filteredArrUsers, cosineSim, train, globalAvg, alluserAvg, preProcessSim)))

      val sim_kNN = allSim.sortBy(-_._2)
      val bestUser = sim_kNN.drop(1).take(k)

  

      bestUser.toMap
  }


  def avgSimilarity_knn(i: Int, u: Int, k: Int, train: Array[Rating], filteredArrUsers: Map[Int, Array[Rating]], globalAvg: Double, 
  alluserAvg: mutable.Map[Int, Double], preProcessSim: mutable.Map[(Int, Int),Double], cosineSim: mutable.Map[(Int, Int),Double], maxUser: Int, arrItem: Map[Int, Array[Rating]]): Double = {

    //val arrFiltered = train.filter(_.item == i)
    val arrFiltered = arrItem.getOrElse(i, Array())
    if(arrFiltered.isEmpty) 0
    else {
      val simANDuser = best_similarities_knn(u, k, train, filteredArrUsers, globalAvg, alluserAvg, preProcessSim, cosineSim, maxUser)
      val res = arrFiltered.foldLeft((0.0, 0.0)){(acc, x) =>
        if (simANDuser.contains(x.user)) (acc._1 + (simANDuser.getOrElse(x.user, 0.0) * dev(x.rating, userAvg(x.user, train, alluserAvg, globalAvg))), acc._2 + simANDuser.getOrElse(x.user, 0.0).abs)
        else (acc._1, acc._2)
        }
      if(res._2 == 0) 0.0
      else res._1 / res._2
    }  
  }





  def predictedPersonalized(user: Int, item : Int, train: Array[Rating], alluserAvg: mutable.Map[Int, Double], globalAvg: Double, 
  allitemAvg : mutable.Map[Int, Double], filteredArrUsers: Map[Int, Array[Rating]], cosineSim: mutable.Map[(Int, Int),Double], preProcessSim : mutable.Map[(Int, Int),Double]): Double = {
    val useravg = userAvg(user, train, alluserAvg, globalAvg)
    if (useravg == globalAvg) globalAvg
    //else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
      val simavgdev = avgSimilarity(item ,user, train, filteredArrUsers, cosineSim, globalAvg, alluserAvg, preProcessSim)
      if (simavgdev == 0) useravg
      else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
    }
  }


  def jaccard_predictedPersonalized(user: Int, item : Int, train: Array[Rating], alluserAvg: mutable.Map[Int, Double], globalAvg: Double, 
  allitemAvg : mutable.Map[Int, Double], filteredArrUsers: Map[Int, Array[Rating]], jaccardSim: mutable.Map[(Int, Int),Double], preProcessSim : mutable.Map[(Int, Int),Double]): Double = {
    val useravg = userAvg(user, train, alluserAvg, globalAvg)
    if (useravg == globalAvg) globalAvg
    //else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
      val simavgdev = jaccard_avgSimilarity(item ,user, train, filteredArrUsers, globalAvg, alluserAvg, jaccardSim)
      if (simavgdev == 0) useravg
      else
      useravg + simavgdev * scale((useravg + simavgdev), useravg)
    }
  }
  

  def predictedPersonalized_knn(user: Int, item : Int, k: Int, train: Array[Rating], alluserAvg: mutable.Map[Int, Double], 
  globalAvg: Double, allitemAvg: mutable.Map[Int, Double], filteredArrUsers: Map[Int, Array[Rating]], cosineSim: mutable.Map[(Int, Int),Double], preProcessSim : mutable.Map[(Int, Int),Double], maxUser: Int, arrItem: Map[Int, Array[Rating]]): Double = {
    
    val useravg = userAvg(user, train, alluserAvg, globalAvg)

    if (useravg == globalAvg) globalAvg
    //else if (itemAvg(item, train, allitemAvg, globalAvg) == globalAvg) useravg
    else {
      val simavgdev = avgSimilarity_knn(item ,user, k, train, filteredArrUsers, globalAvg, alluserAvg, preProcessSim, cosineSim, maxUser, arrItem)
      if (simavgdev == 0) useravg
      else
        useravg + simavgdev * scale((useravg + simavgdev), useravg)
    }
  }

  def recommendation(u: Int, n: Int, k: Int, info: Array[Rating]) : (Array[Double], Array[Int]) = {
      val mapArrUsers = info.groupBy(_.user)
      val arrItem = info.groupBy(_.item)
      val globalAvg2 = computeGlobalAvg(info)
      val maxUser = info.map(elem => elem.user).max
      val arrFiltered = info.filter(x => x.user == u && !(x.rating.isNaN))
      val maxItem = info.map(elem => elem.item).max
      val init = Array.fill(maxItem)(-1.0)
      for (j <- 0 to (maxItem-1)){
        if(init(j) == -1) init(j) = predictedPersonalized_knn(u, j+1, k, info, alluserAvg, globalAvg2, allitemAvg, mapArrUsers, cosineSim, preProcessSim, maxUser, arrItem)
      }
      for(i <- arrFiltered){
        init(i.item-1) = -1 // ENLEVER LES FILMS DEJA VU PAR L'UTILISATEUR
      }
    val out = init.zipWithIndex.sortBy(-_._1).take(n).map(_._2).map(_+1) // le +1 pour avoir numéro d Item pas indice array
    (out.map(x => init(x-1)), out) // LES N RATINGS, LES N ITEMS

  }


  

////
// Predictor & MAE
////


def predictorBaseline(train : Array[Rating]):(Int, Int) => Double = {
    val globalAvg2 = computeGlobalAvg(train)
    val userArr = train.groupBy(_.user).map(x => (x._1, computeGlobalAvg(x._2)))
    val devArr = train.groupBy(_.item).map(x => (x._1, mean_(x._2.map(elem => dev(elem.rating, userArr.getOrElse(elem.user, elem.rating))))))
    (user: Int, item : Int) => {
    val useravg = userArr.getOrElse(user, globalAvg2)
    if (useravg == globalAvg2) globalAvg2
    else {
        val avgdev = devArr.getOrElse(item, 0.0)
        if (avgdev == 0.0) useravg
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
      }
    }
  }

  def predictorGlobal(train : Array[Rating]): (Int, Int) => Double = {
    val globalAvg2 = computeGlobalAvg(train)
    (user, item) => globalAvg2
  }
  
  def predictorUser(train : Array[Rating]): (Int, Int) => Double = {
    val globalAvg2 = computeGlobalAvg(train)
    val userArr = train.groupBy(_.user).map(x => (x._1, computeGlobalAvg(x._2)))
    (user, item) => userArr.getOrElse(user, globalAvg2)
  }

  def predictorItem(train : Array[Rating]): (Int, Int) => Double = {
    val globalAvg2 = computeGlobalAvg(train)
    val itemArr = train.groupBy(_.item).map(x => (x._1, computeGlobalAvg(x._2)))
    (user, item) => itemArr.getOrElse(item, globalAvg2)
  }

  def predictorCosine(train : Array[Rating]): (Int, Int) => Double = {
    val mapArrUsers = train.groupBy(_.user)
    val globalAvg2 = computeGlobalAvg(train)
    (user, item) => predictedPersonalized(user, item, train, alluserAvg, globalAvg2, allitemAvg, mapArrUsers, cosineSim, preProcessSim)
  }

  def predictorJaccard(train : Array[Rating]): (Int, Int) => Double = {
    val mapArrUsers = train.groupBy(_.user)
    val globalAvg2 = computeGlobalAvg(train)
    (user, item) => jaccard_predictedPersonalized(user, item, train, alluserAvg, globalAvg2, allitemAvg, mapArrUsers , jaccardSim, preProcessSim)
  }

  def predictor_knn(train : Array[Rating], k: Int): (Int, Int) => Double = {
    val maxUser = train.map(elem => elem.user).max
    val arrItem = train.groupBy(_.item)
    val mapArrUsers = train.groupBy(_.user)
    val globalAvg2 = computeGlobalAvg(train)
    (user, item) => predictedPersonalized_knn(user, item, k, train, alluserAvg, globalAvg2, allitemAvg, mapArrUsers, cosineSim, preProcessSim, maxUser, arrItem)
  }


  def mae(test: Array[Rating], train: Array[Rating], prediction_method: Array[Rating] => ((Int, Int) => Double)): Double = {
    val predFctn = prediction_method(train)
    mean_(test.map(elem => (predFctn(elem.user, elem.item) - elem.rating).abs))
  }

  def mae_knn(test: Array[Rating], train: Array[Rating], k: Int): Double = {
    val predFctn = predictor_knn(train, k)
    mean_(test.map(elem => (predFctn(elem.user, elem.item) - elem.rating).abs))
  }

}


