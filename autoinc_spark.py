from pyspark import SparkContext
from pyspark.sql import SparkSession

def extract_vin_key_value(line):
    parts = line.strip().split(",")
    if len(parts) < 7:
        return None
    incident_type = parts[1]
    vin = parts[2]
    make = parts[3]
    year = parts[5]
    return (vin, (incident_type, make, year, line))

def populate_make(records):
    make = None
    year = None
    output = []

    # First, find the 'I' record (initial sale) to get make and year
    for rec in records:
        if rec[0] == "I":
            make = rec[1]
            year = rec[2]
            break

    # Then, for all 'A' records, attach make and year
    for rec in records:
        if rec[0] == "A" and make and year:
            output.append((make + "-" + year, 1))

    return output

def main():
    spark = SparkSession.builder.appName("PostSalesReport").getOrCreate()
    sc = spark.sparkContext

    # Replace with your data path if different
    raw_rdd = sc.textFile("data.csv")

    # Remove header
    header = raw_rdd.first()
    filtered_rdd = raw_rdd.filter(lambda row: row != header)

    # Step 1: Extract (vin, details)
    vin_kv = filtered_rdd.map(lambda x: extract_vin_key_value(x)).filter(lambda x: x is not None)

    # Step 2: Group by VIN and enhance accident records with make/year
    enhanced_rdd = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

    # Step 3: Count accidents per (make-year)
    result = enhanced_rdd.reduceByKey(lambda a, b: a + b)

    # Step 4: Save results to output file
    result.map(lambda kv: f"{kv[0]},{kv[1]}").saveAsTextFile("output")

    spark.stop()

if __name__ == "__main__":
    main()
