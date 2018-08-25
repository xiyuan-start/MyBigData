package com.csu

import org.apache.spark._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD


/**
  * Created by luo on 2018/6/11.
  */
//关于银行风险预测
object Credit {

  //用一个scala的case 定义case类的属性
  case class Credit(
                     creditability: Double,
                     balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
                     savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
                     residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
                     credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
                   )

  //用一个函数 解析一行  将值存入Credit类中
  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4) , line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }
  // 将字符串的RDD转换成Double类的RDD
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  //定义main函数
  def main(args: Array[String]): Unit = {
    //定义ScalacAPP
    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取CVS文件
    import sqlContext.implicits._
    //第一个map 将字符串RDD转换成 Double RDD  第二个map  将Double 注入到Credit的类当中
    //toDF 将RDD转换成Credit类的DataFrame(一种表格结构)

    val creditDF = parseRDD(sc.textFile("germancredit.csv")).map(parseCredit).toDF().cache()
    creditDF.registerTempTable("credit")

    //creditDF.printSchema   打印结果
    // creditDF.show
    // 可以利用SQLContext 对数据做进一步SQL操作


    //为了更好地给机器使用对一些特征进行变化   变成一种维度


    val featureCols = Array("balance", "duration", "history", "purpose", "amount",
      "savings", "employment", "instPercent", "sexMarried", "guarantors",
      "residenceDuration", "assets", "age", "concCredit", "apartment",
      "credits", "occupation", "dependents", "hasPhone", "foreign")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(creditDF)

    //在2的基础上增加信用度，这个标签
    //creditability 设置为指标值
    val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)

    //对3的数据进行分割
    //5043random的种子，基本可忽略。
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)






    //第一种方法利用随机森林分类器
    /*
    *MaxDepth 最大深度  越大效果越好，但越延长训练时间
    * NumTrees用于设置数的数量  越大精度越高（考虑维度）
    * maxBins 最大分桶数  决定节点分裂
    * impurity 计算信息增益的指标
    * auto 节点分裂时选择参加的特征数
    * seed 随机生成的种子
    */

    val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)
    val model = classifier.fit(trainingData) //进行训练

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label") //来设置label值
    val predictions = model.transform(testData) //进行预测
    model.toDebugString

    //保存模型
    model.save("BankModel001")





    //计算预测的准确率
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy before pipeline fitting" + accuracy*100+"%")



    /*
    *第二种方法利用管道模式来训练模型（网络搜索法）
    *
    *将不同参数进行组合来预测结果
    *
    * */
   //利用ParamGridBuilder 工具来构建参数网络
    //classifier是已关联好
   val paramGrid = new ParamGridBuilder()
     .addGrid(classifier.maxBins, Array(25, 31))
     .addGrid(classifier.maxDepth, Array(5, 10))
     .addGrid(classifier.numTrees, Array(20, 60))
     .addGrid(classifier.impurity, Array("entropy", "gini"))
     .build()

    //创建管道 由一系列stage  每个stage相当于一个 Estimator测评者或者Transformer转换者
    val steps: Array[PipelineStage] = Array(classifier)
    val pipeline = new Pipeline().setStages(steps)

    //用CrossValidator类完成模型筛选  不能过高
    // 这类让管道在网上爬行
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    //管道在参数网络上爬行不断被优化
    val pipelineFittedModel = cv.fit(trainingData)
    pipelineFittedModel.save("BankPipelineMode")

    //测试数据
    val predictions2 = pipelineFittedModel.transform(testData)
    val accuracy2 = evaluator.evaluate(predictions2)
    println("accuracy after pipeline fitting" + accuracy2*100+"%")






  }


}

