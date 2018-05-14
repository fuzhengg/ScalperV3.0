package com.blTest

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.sql.DataFrame
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Vectors,Vector}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
object LogisticRegression3{
  //屏蔽不必要的日志显示在终端上
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

//  val conf = new SparkConf().setMaster("local").setAppName("LogisticRegression3")         //设置环境变变量
//  val sc = new SparkContext(conf)


  def getLabeledData(sparkSql: SparkSession):org.apache.spark.sql.DataFrame = {
    //y=1:Hn; y=2:NO HN
    val iniData = sparkSql.sql(" SELECT a.member_id " +
      ",case when b.circle_memberid is null then 0 else 1 end as HnFlag  " +
      ",a.ipphonenum,a.ipnamenum,a.ipaddressnum,a.devicenamenum,a.devicephonenum,a.deviceaddressnum,a.devmemnum,a.phonememnum,a.sameaddressmemnum,a.ipmemnum,a.namememnum " +
      "FROM  (select *  from   algorithm_data.HnMember_info  ) a  " +
      "left join (select distinct circle_memberid  from algorithm_data.hnCircleScore where  year=2018 and month=4 and day=25  and category='20180322nonRealTime' ) b on a.member_id=b.circle_memberid     ")
//    val data = iniData.select("HnFlag", "ipphonenum", "ipnamenum", "ipaddressnum", "devicenamenum", "devicephonenum", "deviceaddressnum", "devmemnum", "phonememnum", "sameaddressmemnum", "ipmemnum", "namememnum")
    val data = iniData.select("HnFlag",  "ipphonenum", "ipnamenum", "ipaddressnum", "devicenamenum", "devicephonenum", "deviceaddressnum", "devmemnum", "phonememnum", "sameaddressmemnum", "ipmemnum", "namememnum")
    val colArray2=Array(  "ipphonenum", "ipnamenum", "ipaddressnum", "devicenamenum", "devicephonenum", "deviceaddressnum", "devmemnum", "phonememnum", "sameaddressmemnum", "ipmemnum", "namememnum")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(data)
    vecDF
  }





  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionTest").set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val sc = new SparkContext(conf)
    val sparkSql = SparkSession.builder.enableHiveSupport.getOrCreate()

//    val data = MLUtils.loadLibSVMFile(sc, "E://wa1.txt") //设置数据集
    //val data = MLUtils.loadLabeledPoints(sc,"E://wa.txt")

    val labeledData= getLabeledData(sparkSql)
    /**
      * 首先介绍一下 libSVM的数据格式
        Label 1:value 2:value ….
        Label：是类别的标识
        Value：就是要训练的数据，从分类的角度来说就是特征值，数据之间用空格隔开
        比如: -15 1:0.708 2:1056 3:-0.3333
      */

//    import org.apache.spark.ml.feature.PCA
//    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(5).fit(labeledData)
//    val pcaDF = pca.transform(labeledData)



    val Array(trainingDF,testDF) = labeledData.randomSplit(Array(0.9,0.1),seed = 1234) //对数据集切分成两部分，一部分训练模型，一部分校验模型


//决策树
    import org.apache.spark.mllib.tree.RandomForest
    import org.apache.spark.mllib.tree.model.RandomForestModel

val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingDF,numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")






    //逻辑回归
    //splits.foreach(println)
    val lrModel = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setStandardization(true).setThreshold(0.9).setLabelCol("HnFlag").setFeaturesCol("features").fit(trainingDF)

    // 输出逻辑回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    lrModel.numFeatures

    val trainingSummary = lrModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

//    lrModel.transform(testDF).show
    lrModel.transform(testDF).select("HnFlag","features","probability","prediction").show(30)












    val numiteartor = 50
//    val model = LogisticRegressionWithSGD.train(trainingDF,numiteartor) //训练模型
//    println(model.weights)

    val predictionAndLabels = testDF.map{                           //计算测试值
      case LabeledPoint(label,features) =>
        val prediction = model.predict(features)
        (prediction,label)                                              //存储测试值和预测值
    }
    predictionAndLabels.foreach(println)
    val trainErr = predictionAndLabels.filter( r => r._1 != r._2).count.toDouble / testDF.count
    println("容错率为trainErr： " +trainErr)
    /**
      * 容错率为trainErr： 0.3
      */

    val metrics = new MulticlassMetrics(predictionAndLabels)           //创建验证类
    val precision = metrics.precision                                   //计算验证值
    println("Precision= "+precision)

    val patient = Vectors.dense(Array(70,3,180.0,4,3))                  //计算患者可能性
    if(patient == 1)println("患者的胃癌有几率转移。 ")
    else println("患者的胃癌没有几率转移 。")
    /**
      * Precision= 0.7
      患者的胃癌没有几率转移
      */
  }
}
