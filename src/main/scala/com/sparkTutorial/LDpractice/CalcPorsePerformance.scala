import sparkSubmit.SparkSetupClass
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter
import org.slf4j.MDC

package porseCalc {

  object CalcPorsePerformance extends App {

    val t0 = System.nanoTime()

    import org.apache.spark.sql.functions._
    import java.sql.{Date}

    val logger = LoggerFactory.getLogger(CalcPorsePerformance.getClass())
    logger.info("PorseCalculation initiated")

    // print internal state
    //val lc: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    //   StatusPrinter.print(lc)

    /* if (logger.isDebugEnabled) {
       logger.debug("event Debug {}", CalcPorsePerformance.getClass().getSimpleName)
       logger.error("Error loging test")
     }
 */
    //if (!args.isEmpty && args.length >= 1) {
    try {
      val x = new SparkSetupClass
      val spark = x.createSpark("porsePerformanceCalculation")

      //val date_arg = args(0).toString() // have to come as parameter
      val od_popn_dts = current_timestamp()
      //val date_limit: Date = Date.valueOf(date_arg) //date where is start position "2017-01-01"
      //val date_limit: Date = Date.valueOf("2017-01-01")

      import spark.implicits._

      spark.conf.set("spark.debug.maxToStringFields", "300")
      logger.info("Starting to read porse_stage.stg_porse_consumable_performance_info")
      var df = spark.sql("select tbl1.depotid" +
        ",tbl1.performancedate" +
        ",tbl1.portfolioresponsibleid" +
        ",tbl1.portfolioresponsible" +
        ",tbl1.officename" +
        ",tbl1.profitcenter" +
        ",tbl1.isdescretionary" +
        ",tbl1.depottype" +
        ",tbl1.depottypename" +
        ",tbl1.performancevalue_total" +
        ",tbl1.performancevalue_swedisheq" +
        ",tbl1.performancevalue_globaleq" +
        ",tbl1.performancevalue_fixedincome" +
        ",tbl1.performancevalue_alternatives" +
        ",tbl1.performancevalue_cash " +
        ",tbl1.marketvalue_total" +
        ",tbl1.marketvalue_swedisheq" +
        ",tbl1.marketvalue_globaleq" +
        ",tbl1.marketvalue_fixedincome" +
        ",tbl1.marketvalue_alternatives" +
        ",tbl1.marketvalue_cash" +
        ",tbl1.benchmark_startdate" +
        ",tbl1.benchmarkname_swedisheq" +
        ",tbl1.benchmarkname_globaleq" +
        ",tbl1.benchmarkname_fixedincome" +
        ",tbl1.benchmarkname_alternatives" +
        ",tbl1.benchmarkname_cash" +
        ",tbl1.benchmark_assetclass_weight_swedisheq" +
        ",tbl1.benchmark_assetclass_weight_globaleq" +
        ",tbl1.benchmark_assetclass_weight_fixedincome" +
        ",tbl1.benchmark_assetclass_weight_alternatives" +
        ",tbl1.benchmark_assetclass_weight_cash" +
        ",tbl1.benchmark_performanceret_total" +
        ",tbl1.benchmark_performanceret_swedisheq" +
        ",tbl1.benchmark_performanceret_globaleq" +
        ",tbl1.benchmark_performanceret_fixedincome" +
        ",tbl1.benchmark_performanceret_alternatives" +
        ",tbl1.benchmark_performanceret_cash" +
        ",tbl1.bd_dts" +
        ",tbl1.limit_date as date_limit" +
        ",tbl1.od_popn_dts as od_load_dts " +
        "from porse_stage.stg_porse_consumable_performance_info tbl1 " +
        "order by tbl1.depotid, tbl1.performancedate").coalesce(1)

      logger.info("porse_stage.stg_porse_consumable_performance_info table read finished")
      //df = df.withColumn("date_limit", lit(date_limit))
      df = df.withColumn("od_popn_dts", lit(od_popn_dts))

      df.createOrReplaceTempView("myPerformanceTmpTbl")
      logger.info("createOrReplaceTempView -> myPerformanceTmpTbl created")

      val dfw1 = spark.sql("select depotid, performancedate, " +
        "performancevalue_total, " +
        "performancevalue_swedisheq, " +
        "performancevalue_globaleq, " +
        "performancevalue_fixedincome, " +
        "performancevalue_alternatives, " +
        "performancevalue_cash, " +
        "benchmark_startdate, " +
        "benchmark_assetclass_weight_swedisheq, " +
        "benchmark_assetclass_weight_globaleq as benchmark_assetclass_weight_total, " +
        "benchmark_assetclass_weight_globaleq, " +
        "benchmark_assetclass_weight_fixedincome, " +
        "benchmark_assetclass_weight_alternatives, " +
        "benchmark_assetclass_weight_cash, " +
        "COALESCE(benchmark_performanceret_total,1) as benchmark_performanceret_total, " +
        "COALESCE(benchmark_performanceret_swedisheq,1) as benchmark_performanceret_swedisheq, " +
        "COALESCE(benchmark_performanceret_globaleq,1) as benchmark_performanceret_globaleq, " +
        "COALESCE(benchmark_performanceret_fixedincome,1) as benchmark_performanceret_fixedincome, " +
        "COALESCE(benchmark_performanceret_alternatives,1) as benchmark_performanceret_alternatives, " +
        "COALESCE(benchmark_performanceret_cash,1) as benchmark_performanceret_cash, " +
        "bd_dts, od_popn_dts, od_load_dts, " +
        "date_limit, " +
        "case when (benchmark_startdate is null and lead(benchmark_startdate, 1) over (partition by depotid ORDER BY performancedate) is not null) or (performancedate < date_limit and lead(performancedate, 1) over (partition by depotid ORDER BY performancedate) >= date_limit and benchmark_startdate is not null) then 1 else 0 end as startPos_all, " +
        "lag(depotid, 1) over (partition by depotid ORDER BY performancedate) as prev_depotid " +
        "from myPerformanceTmpTbl")
      logger.info("DataFrame dfw1 for calculations created from myPerformanceTmpTbl")
      var prevValue_total: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_total: java.lang.Double = null.asInstanceOf[java.lang.Double]

      var prevValue_swedisheq: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_swedisheq: java.lang.Double = null.asInstanceOf[java.lang.Double]

      var prevValue_globaleq: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_globaleq: java.lang.Double = null.asInstanceOf[java.lang.Double]

      var prevValue_fixedincome: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_fixedincome: java.lang.Double = null.asInstanceOf[java.lang.Double]

      var prevValue_alternatives: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_alternatives: java.lang.Double = null.asInstanceOf[java.lang.Double]

      var prevValue_cash: java.lang.Double = null.asInstanceOf[java.lang.Double]
      var initValue_cash: java.lang.Double = null.asInstanceOf[java.lang.Double]
      logger.info("Starting DataFrame dfw1 map to calculate 5 caclulated fields")
      var dfCalc = dfw1.map(r => {
        var rr_total = r.get(r.fieldIndex("benchmark_performanceret_total")).asInstanceOf[java.lang.Double]
        var rr_swedisheq = r.get(r.fieldIndex("benchmark_performanceret_swedisheq")).asInstanceOf[java.lang.Double]
        var rr_globaleq = r.get(r.fieldIndex("benchmark_performanceret_globaleq")).asInstanceOf[java.lang.Double]
        var rr_fixedincome = r.get(r.fieldIndex("benchmark_performanceret_fixedincome")).asInstanceOf[java.lang.Double]
        var rr_alternatives = r.get(r.fieldIndex("benchmark_performanceret_alternatives")).asInstanceOf[java.lang.Double]
        var rr_cash = r.get(r.fieldIndex("benchmark_performanceret_cash")).asInstanceOf[java.lang.Double]
        //--------
        var depotid = r.getString(r.fieldIndex("depotid"))
        var prev_depotid = r.getString(r.fieldIndex("prev_depotid"))

        var startPos_general = r.get(r.fieldIndex("startPos_all")).asInstanceOf[Int]

        //-------------total-------------
        if (startPos_general == 1) {
          initValue_total = r.get(r.fieldIndex("performancevalue_total")).asInstanceOf[java.lang.Double]
          prevValue_total = initValue_total
        }
        else {
          if (Option(prevValue_total).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_total")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_total = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_total = prevValue_total * rr_total
            }
          }
          else {
            prevValue_total = null.asInstanceOf[java.lang.Double]
          }
        }

        //-------------swedisheq-------------
        if (startPos_general == 1) {
          initValue_swedisheq = r.get(r.fieldIndex("performancevalue_swedisheq")).asInstanceOf[java.lang.Double]
          prevValue_swedisheq = initValue_swedisheq
        }
        else {
          if (Option(prevValue_swedisheq).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_swedisheq")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_swedisheq = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_swedisheq = prevValue_swedisheq * rr_swedisheq
            }
          }
          else {
            prevValue_swedisheq = null.asInstanceOf[java.lang.Double]
          }
        }
        //---------globaleq-------
        if (startPos_general == 1) {
          initValue_globaleq = r.get(r.fieldIndex("performancevalue_globaleq")).asInstanceOf[java.lang.Double]
          prevValue_globaleq = initValue_globaleq
        }
        else {
          if (Option(prevValue_globaleq).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_globaleq")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_globaleq = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_globaleq = prevValue_globaleq * rr_globaleq
            }
          }
          else {
            prevValue_globaleq = null.asInstanceOf[java.lang.Double]
          }
        }
        //---------------fixedincome------------
        if (startPos_general == 1) {
          initValue_fixedincome = r.get(r.fieldIndex("performancevalue_fixedincome")).asInstanceOf[java.lang.Double]
          prevValue_fixedincome = initValue_fixedincome
        }
        else {
          if (Option(prevValue_fixedincome).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_fixedincome")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_fixedincome = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_fixedincome = prevValue_fixedincome * rr_fixedincome
            }
          }
          else {
            prevValue_fixedincome = null.asInstanceOf[java.lang.Double]
          }
        }
        //-------------alternatives-----
        if (startPos_general == 1) {
          initValue_alternatives = r.get(r.fieldIndex("performancevalue_alternatives")).asInstanceOf[java.lang.Double]
          prevValue_alternatives = initValue_alternatives
        }
        else {
          if (Option(prevValue_alternatives).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_alternatives")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_alternatives = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_alternatives = prevValue_alternatives * rr_alternatives
            }
          }
          else {
            prevValue_alternatives = null.asInstanceOf[java.lang.Double]
          }
        }
        //-------------cash ---------------------
        if (startPos_general == 1) {
          initValue_cash = r.get(r.fieldIndex("performancevalue_cash")).asInstanceOf[java.lang.Double]
          prevValue_cash = initValue_cash
        }
        else {
          if (Option(prevValue_cash).isDefined) //Null value for double
          {
            if (r.get(r.fieldIndex("benchmark_assetclass_weight_cash")) == null.asInstanceOf[BigDecimal] || !depotid.equals(prev_depotid)) {
              prevValue_cash = null.asInstanceOf[java.lang.Double]
            }
            else {
              prevValue_cash = prevValue_cash * rr_cash
            }
          }
          else {
            prevValue_cash = null.asInstanceOf[java.lang.Double]
          }
        }
        //-------------------------
        (depotid,
          r.getTimestamp(r.fieldIndex("performancedate")),
          prevValue_total,
          prevValue_swedisheq,
          prevValue_globaleq,
          prevValue_fixedincome,
          prevValue_alternatives,
          prevValue_cash,
          r.getTimestamp(r.fieldIndex("bd_dts")),
          r.getTimestamp(r.fieldIndex("od_popn_dts")),
          r.getTimestamp(r.fieldIndex("od_load_dts")),
          startPos_general,
          r.getTimestamp(r.fieldIndex("date_limit")))
      }).toDF("depotid", "performancedate", "benchmark_performancevalue_total", "benchmark_performancevalue_swedisheq", "benchmark_performancevalue_globaleq", "benchmark_performancevalue_fixedincome", "benchmark_performancevalue_alternatives", "benchmark_performancevalue_cash", "bd_dts", "od_popn_dts", "od_load_dts", "startPos_general", "date_limit").filter("performancedate >= date_limit")

      logger.info("DataFrame dfw1 map to calculate 5 caclulated fields finished dfCalc created")

      dfCalc.createOrReplaceTempView("perfAfterCalculation")

      logger.info("createOrReplaceTempView -> perfAfterCalculation created ")
      logger.info("Starting insert overwrite table porse_consumable.performance_info (joining perfAfterCalculation and myPerformanceTmpTbl temp views)")

      spark.sql("INSERT OVERWRITE TABLE porse_consumable.performance_info select tbl1.depotid " +
        ",tbl1.performancedate " +
        ",tbl1.portfolioresponsibleid " +
        ",tbl1.portfolioresponsible " +
        ",tbl1.officename " +
        ",tbl1.profitcenter " +
        ",tbl1.isdescretionary " +
        ",tbl1.depottype " +
        ",tbl1.depottypename " +
        ",tbl1.performancevalue_total " +
        ",tbl1.performancevalue_swedisheq " +
        ",tbl1.performancevalue_globaleq " +
        ",tbl1.performancevalue_fixedincome " +
        ",tbl1.performancevalue_alternatives " +
        ",tbl1.performancevalue_cash " +
        ",tbl1.marketvalue_total " +
        ",tbl1.marketvalue_swedisheq " +
        ",tbl1.marketvalue_globaleq " +
        ",tbl1.marketvalue_fixedincome " +
        ",tbl1.marketvalue_alternatives " +
        ",tbl1.marketvalue_cash " +
        ",tbl1.benchmark_startdate " +
        ",tbl1.benchmarkname_swedisheq " +
        ",tbl1.benchmarkname_globaleq " +
        ",tbl1.benchmarkname_fixedincome " +
        ",tbl1.benchmarkname_alternatives " +
        ",tbl1.benchmarkname_cash " +
        ",tbl1.benchmark_assetclass_weight_swedisheq " +
        ",tbl1.benchmark_assetclass_weight_globaleq " +
        ",tbl1.benchmark_assetclass_weight_fixedincome " +
        ",tbl1.benchmark_assetclass_weight_alternatives " +
        ",tbl1.benchmark_assetclass_weight_cash " +
        ",tbl1.benchmark_performanceret_total " +
        ",tbl1.benchmark_performanceret_swedisheq " +
        ",tbl1.benchmark_performanceret_globaleq " +
        ",tbl1.benchmark_performanceret_fixedincome " +
        ",tbl1.benchmark_performanceret_alternatives " +
        ",tbl1.benchmark_performanceret_cash " +
        ",tbl2.benchmark_performancevalue_total" +
        ",tbl2.benchmark_performancevalue_swedisheq" +
        ",tbl2.benchmark_performancevalue_globaleq" +
        ",tbl2.benchmark_performancevalue_fixedincome" +
        ",tbl2.benchmark_performancevalue_alternatives" +
        ",tbl2.benchmark_performancevalue_cash" +
        ",tbl2.bd_dts " +
        ",tbl2.od_load_dts " +
        ",tbl2.od_popn_dts from myPerformanceTmpTbl tbl1 inner join perfAfterCalculation tbl2 on tbl1.depotid=tbl2.depotid and tbl1.performancedate=tbl2.performancedate ")
      logger.info("Insert overwrite table porse_consumable.performance_info is done")
      logger.info("----- SUCCESS ----")
    } catch {
      case e: Exception =>
        logger.error("ERROR while running job {}", e.printStackTrace())
        //println(e.printStackTrace())
        sys.exit(1)
    }

    val t1 = System.nanoTime()
    logger.info("----- Execution time in seconds -----> {}", (t1 - t0).asInstanceOf[Double] / 1000000000)
    //-----save as CSV or table
    //dfCalc.write.option("compression","none").option("delimiter",";").option("header", "true").mode("overwrite").csv("/tmp/outcalc")
    //dfCalc.write.mode("overwrite").saveAsTable("default.performance_calc")


  }

}