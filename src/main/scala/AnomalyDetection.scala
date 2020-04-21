package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.saddle.vec.VecDouble
import org.saddle.{Vec, na, vec}

object AnomalyDetection {
    def main(args: Array[String]): Unit = {
        val voltage = Array[Double](4.02,4.02,4.02,4.02,4.02,4.09,4,4.02,4.02,4,4.02,4.02,4.02,4.02,4.02,4.02,
            4.02,4.02,4,4.02,4.02,4,4.02,4.02,4.02,4.02,4.02,4.02,4,4,4.02,4.02,4.02,4.02,4.02,4.02,
            4.02,4,4.02,4.02,4.1,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4,4.02,4.02,4.02,4.02,4.02,4.02,
            4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4,4.02,4.02,4.02,4.02,4.02,4.02,
            4.02,4.02,4.02,4,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02,4.02)
        val fault_matrix = nSigmaOutlierDetect(voltage)
        println(fault_matrix.toSeq.mkString(","))

        // Using Spark
        Logger.getLogger("org").setLevel(Level.ERROR)

        val masterUrl = "local[*]"
        val dataPath = "data/"

        val conf = new SparkConf().setMaster(masterUrl).setAppName("VoltageFaultCSVLocalTest")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val sc = spark.sparkContext

        // Read test data
        val voltageRDD = sc.textFile(dataPath + "voltage.csv")

        val faultRDD = voltageRDD.map(line => line.split(","))
            .map(x => (x(0),x.slice(1,x.length)))                 // str(vin) - str(vol)
            .map(x => (x._1,new VecDouble(x._2.map(_.toDouble)))) // str(vin) - VecDouble(vol)
            .map(x => (x._1,(nSigmaOutlierDetect(x._2),1)) )      // str(vin) - Vec(fault),1
            .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
            .map(x => (x._1,x._2._1 / x._2._2))                   // str(vin) - Mean(Vec(fault))
            .map(x=>{
                val r1 = x._1.toString
                val r2 = x._2.toSeq.mkString(",")                 // VecDouble(fault) => Vector(fault) => String(fault)
                r1 + "," + r2
            })
            .foreach(println)
        spark.stop()
    }

    /**
     *
     * @param raw_data:voltage array
     * @return fault_matrix
     */
    def nSigmaOutlierDetect(raw_data: Array[Double]): Vec[Double] = {
        // n*Sigma (default n=3)
        val n = 3
        // Repeat three times
        val repeat = 3
        // Create a copy
        val data = new VecDouble(raw_data).copy
        // Initialize fault matrix
        val fault_matrix = vec.zeros(data.length)

        for (each <- 1 until repeat) {
            // Calculate the mean
            val data_mean = data.mean
            // Calculate the std
            val data_stdev = data.stdev

            // Lower threshold
            val threshold1 = data_mean - n * data_stdev
            // Higher threshold
            val threshold2 = data_mean + n * data_stdev

            for (i <- 0 until data.length) {
                // the value is abnormal
                if ((data.at(i) < threshold1) || (data.at(i) > threshold2)) {
                    // Out of the range, fault matrix returns 1.
                    // Within the range, fault matrix returns 0.
                    fault_matrix(i) = 1
                    // Remove outliers
                    data(i) = na.to[Double]
                }
            }
        }
        fault_matrix
    }
}
