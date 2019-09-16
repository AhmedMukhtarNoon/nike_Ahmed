from pyspark.sql.functions import *

master_df = sqlContext.read.load('XTRA_MSTR.csv', format='com.databricks.spark.csv', header='true', inferSchema='true').cache()

sold_df = sqlContext.read.load('XTRA_SOLD.csv', format='com.databricks.spark.csv', header='true', inferSchema='true').dropDuplicates().cache()

left_join = sold_df.select(col("XS_Key"), col("XS_Date"), col("XS_Quantity"), col("XS_Loc_x"), col("XS_Loc_y")) \
  .join(master_df.select(col("XM_Key"), col("XM_Name")), col("XS_Key") == col("XM_Key"),how='left')

temp_df = left_join.select(col("XS_Date").alias("SOLD_DATE"), col("XM_Name").alias("SOLD_Product"), col("XS_Quantity").alias("SOLD_Quantity"), col("XS_Loc_x"), col("XS_Loc_y"))

result_df = temp_df.withColumn('SOLD_Location', concat(lit("("), temp_df.XS_Loc_x, lit(","), temp_df.XS_Loc_y, lit(")"))).drop("XS_Loc_x","XS_Loc_y")

result_df.repartition(1).write.csv("XTRA_DATA.csv", sep=',')
