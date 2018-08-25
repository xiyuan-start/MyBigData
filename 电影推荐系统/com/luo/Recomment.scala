package com.luo


import java.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}



object Recomment {


  def main(args: Array[String]): Unit = {


    //建立spark环境
    val conf = new SparkConf().setAppName("movieRecomment")

    val sc = new SparkContext(conf)

    //去读文件并且进行预处理
    val ratings = sc.textFile("ratings.dat").map {
      line =>
        val fields = line.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
      //时间戳、用户编号、电影编号、评分
      //表中已预设名称了
    }



      val movies = sc.textFile("movies.dat").map { line =>
        val fields = line.split("::")
        // format: (movieId, movieName)
        (fields(0).toInt, fields(1))
      }.collect.toMap

    //记录数、用户数、电影数
    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count
    println("从" + numRatings + "记录中" + "分析了" + numUsers + "的人观看了" + numMovies + "部电影")



    //提取一个得到最多评分的电影子集，以便进行评分启发
    //矩阵最为密集的部分

    val mostRatedMovieIds = ratings.map(_._2.product)
      .countByValue()
      .toSeq
      .sortBy(-_._2)
      .take(50) //50个
      .map(_._1) //获取他们的id


    val random = new Random(0)
    val selectedMovies = mostRatedMovieIds.filter(
      x => random.nextDouble() < 0.2).map(x => (x, movies(x))).toSeq

    //引导或者启发评论
    //调用函数   从目前最火的电影中随机获取十部电影
    //让用户打分
    val myRatings = elicitateRatings(selectedMovies)
    val myRatingsRDD = sc.parallelize(myRatings)


    //将评分系统分成训练集60%，验证集20%，测试集20%
    val numPartitions = 20
    //训练集
    val training = ratings.filter(x => x._1 < 6).values
      .union(myRatingsRDD).repartition(numPartitions)
      .persist
    //验证集
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values
      .repartition(numPartitions).persist
    //测试集
    val test = ratings.filter(x => x._1 >= 8).values.persist
    val numTraining = training.count
    val numValidation = validation.count
    val numTest = test.count
    println("训练集数量:" + numTraining + ",验证集数量: " + numValidation + ", 测试集数量:" + numTest)


    //训练模型，并且在验证集上评估模型
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation)=" + validationRmse + "for the model trained with rand =" + rank + ", lambda=" + lambda + ", and numIter= " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    //在测试集中 获得最佳模型
    val testRmse = computeRmse(bestModel.get, test, numTest)
    println("The best model was trained with rank=" + bestRank + " and lambda =" + bestLambda + ", and numIter =" + bestNumIter + ", and itsRMSE on the test set is" + testRmse + ".")


    //产生个性化推荐
    val myRateMoviesIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRateMoviesIds.contains(_)).toSeq)
    val recommendations = bestModel.get.predict(candidates.map((0, _)))
      .collect()
      .sortBy((-_.rating))
      .take(50)
    var i = 1
    println("以下电影推荐给你")
    recommendations.foreach { r =>
      println("%2d".format(i) + ":" + movies(r.product))
      i += 1

    }





  }
  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }


  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    val prompt = "给以下电影评分（1——5分）"
    println(prompt)
    val ratings = movies.flatMap { x =>
      var rating: Option[Rating] = None
      var valid = false
      while (!valid) {
        print(x._2 + ": ")
        try {
          val r = Console.readInt
          if (r < 0 || r > 5) {
            println(prompt)
          } else {
            valid = true
            if (r > 0) {
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    if (ratings.isEmpty) {
      error("No rating provided!")
    } else {
      ratings
    }

  }
}
