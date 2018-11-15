package StreamingProject.domain

/**
  * Created by Administrator on 2018/9/25.
  * 实战课程点击数
  */
/**
  *
  * @param day_course 对应的就是HBase中的rowkey，20171111_1
  * @param click_count  对应20171111_1的访问总数
  */
case class CourseClickCount(day_course:String,click_count:Long)
