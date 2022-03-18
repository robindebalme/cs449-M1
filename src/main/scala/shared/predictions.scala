package shared
import scala.collection.mutable

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

  var alluserAvg : mutable.Map[Int, Double] = mutable.Map()
  var allitemAvg : mutable.Map[Int, Double] = mutable.Map()
  var allitemDev : mutable.Map[Int, Double] = mutable.Map()
  var cosineSim : mutable.Map[(Int, Int),Double] = mutable.Map()
  var preProcessSim : mutable.Map[(Int, Int),Double] = mutable.Map()
  var commonItemMap: mutable.Map[(Int, Int),Array[Int]] = mutable.Map()
  var mapArrUsers : Map[Int, Array[Rating]] =  Map()

  var globalAvg = 0.0

  def mean_(arr : Array[Double]): Double = {
    val tmp = arr.foldLeft((0.0, 0))((acc, elem) => (acc._1 + elem, acc._2 + 1))
    tmp._1 / tmp._2
  }

  def computeGlobalAvg(train : Array[Rating]): Double = {
    mean_(train.map(_.rating))
  }

  def userAvg(user : Int, train : Array[Rating], alluserAvg : mutable.Map[Int, Double], globalAvg : Double): Double = {
    if (alluserAvg.get(user) != None) 
      alluserAvg(user)
    else 
      {
      val filtered = train.filter(elem => elem.user == user)
      var tmp = 0.0
      if (filtered.isEmpty)
        tmp = globalAvg
      else
        tmp = mean_(filtered.map(_.rating))

      alluserAvg += ((user, tmp))
      tmp
    }
  }


  def itemAvg(item : Int, train : Array[Rating], allitemAvg : mutable.Map[Int, Double], globalAvg : Double): Double = {
    if (allitemAvg.get(item) != None) 
      allitemAvg(item)
    else 
      {
      val filtered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (filtered.isEmpty)
        tmp = globalAvg
      else
        tmp = mean_(filtered.map(_.rating))

      allitemAvg += ((item, tmp))
      tmp
    }
  }

  def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  def dev(r: Double, useravg : Double): Double = {
    (r - useravg) / scale(r, useravg)
  }

  def itemAvgDev(item : Int, train : Array[Rating], allitemDev : mutable.Map[Int, Double], globalAvg : Double, alluserAvg : mutable.Map[Int, Double]): Double = {
    if (allitemDev.get(item) != None) 
      allitemDev(item)
    else {
      var tmp = train.filter(l => l.item == item)
      var tmp2 = mean_(tmp.map(elem => dev(elem.rating, userAvg(elem.user, train, alluserAvg, globalAvg))))
      allitemDev +=  ((item, tmp2))
      tmp2
    }
  }

  def predictedBaseline(user: Int, item : Int, train : Array[Rating], allitemDev : mutable.Map[Int, Double], globalAvg : Double, alluserAvg : mutable.Map[Int, Double], allitemAvg : mutable.Map[Int, Double]): Double = {
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
        val avgdev = itemAvgDev(item, train, allitemDev, globalAvg, alluserAvg)
        if (avgdev == 0) {
          useravg
        }
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
      }
    }
  }

  def predictorGlobal(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => globalAvg
  }

  def predictorUser(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => userAvg(user, train, alluserAvg, globalAvg)
  }

  def predictorItem(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => itemAvg(item, train, allitemAvg, globalAvg)
  }

  def predictorBaseline(train : Array[Rating]): (Int, Int) => Double = {
    (user, item) => predictedBaseline(user, item, train, allitemDev, globalAvg, alluserAvg, allitemAvg)
  }

  def mae(test: Array[Rating], train: Array[Rating], prediction_method: Array[Rating] => ((Int, Int) => Double)): Double = {
    mean_(test.map(elem => (prediction_method(train)(elem.user, elem.item) - elem.rating).abs))
  }

}


