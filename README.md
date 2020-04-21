# 3sigmaDetection

My attempt at reproducing the paper Fault and defect diagnosis of battery for electric vehicles based on big data analysis methods. 

Let me know if there are any bugs in my code.

I implemented  3sigma multi-level screening strategy on Scala 2.11 and Spark 2.4.3.

## DataSet
To protect privacy, I randomly generated some battery cell voltage data.

## Run
run AnomalyDetection

## Detail
def nSigmaOutlierDetect(raw_data: Array[Double]): Vec[Double] = {
        // nSigma (default n=3)
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
