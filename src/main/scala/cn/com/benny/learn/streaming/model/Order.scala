/*
 * CopyRight benny
 * ProjectName: mybatis-generator-learn
 * Author: benny
 * Date: 18-12-23 上午12:33
 * LastModified: 18-12-22 下午10:46
 */

package cn.com.benny.learn.streaming.model

import java.util.Date
;

/**
 * <p>Description:<p>
 * QQ: 178542285
 *
 * @Author benny
 * @Date: 2018/12/22
 * @Time: 20:49
 */
case class Order( payer:String,  payee:String, orderNo:String, amont:Int,actAmont:Int, category:String,  remark:String, createTime:Date)
