val df = spark.read.option("header", "true").csv("s3://sparknew/DelayedFlights-updated.csv");

df.write.saveAsTable("delay_flights")

val results = spark.sql("SELECT Year from delay_flights");
results.show()

val CarrierDelay = spark.sql("SELECT Year, avg((CarrierDelay /ArrDelay)*100) AS CarrierDelay  FROM  delay_flights GROUP BY Year")
spark.time(CarrierDelay.show())

val NASDelay = spark.sql("SELECT Year, avg((NASDelay /ArrDelay)*100) FROM delay_flights GROUP BY Year")
spark.time(NASDelay.show())

val WeatherDelay = spark.sql("SELECT Year, avg((WeatherDelay /ArrDelay)*100) FROM delay_flights GROUP BY Year")
spark.time(WeatherDelay.show())

val LateAircraftDelay = spark.sql("SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) FROM delay_flights GROUP BY Year")
spark.time(LateAircraftDelay.show())

val SecurityDelay = spark.sql("SELECT Year, avg((SecurityDelay /ArrDelay)*100) FROM delay_flights GROUP BY Year")
spark.time(SecurityDelay.show())
