package StreamingProject

import _0924MoocProject.dao.{CourseClickCountDao, CourseSearchClickCountDao}
import _0924MoocProject.domain.{Clicklog, CourseClickCount, CourseSearchClickCount}
import _0924MoocProject.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/9/24.
  */
object ImoocStatStreamingApp_product {
  def main(args: Array[String]): Unit = {

    if(args.length!=4){
      println("Usage : ImoocStatStreamingApp<zkQuorum><group><topics><numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum,group,topics,numThreads)=args

    val sparkConf=new SparkConf().setAppName("ImoocStatStreamingApp")
      //.setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap

    //获得kafka信息
    val messages=KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

    //测试步骤一：测试数据接收
//    messages.map(_._2).count().print()

    //测试步骤二：数据清洗
    val logs =messages.map(_._2)
    val cleanData=logs.map(line=>{
      val infos=line.split("\t")
      //info(2)=" GET /class/112.html HTTP/1.1"

      val url=infos(2).split(" ")(1)
      var courseId=0
      if(url.startsWith("/class")){
        val courseIdHTML=url.split("/")(2)
        courseId= courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
      }

      Clicklog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))

    })//.filter(Clicklog=>Clicklog.courseId!=0) //过滤

//    cleanData.print()

    //测试步骤三：统计今天到现在为止实战课程的访问量

    cleanData.map(x=>{
      //HBase rowkey设计：20171111_08
      (x.time.substring(0,8)+"_"+x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list=new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })

        CourseClickCountDao.save(list)
      })
    })

    //测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    //http://www.baidu.com/s?wd=大数据面试
    cleanData.map(x=>{
      /**
        * http://www.baidu.com/s?wd=大数据面试
        * ==>转换为==>
        * ttp:/www.baidu.com/s?wd=大数据面试
        */
      val referer=x.referer.replaceAll("//","/")
      val splits=referer.split("/")
      var host=""
      if(splits.length>2){
        host=splits(1)
      }
      (host,x.courseId,x.time)
    }).filter(_._1!="").map(x=>{
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list=new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseSearchClickCount(pair._1,pair._2))
        })

        CourseSearchClickCountDao.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
