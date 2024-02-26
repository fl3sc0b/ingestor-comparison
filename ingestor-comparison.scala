// Databricks notebook source
dbutils.widgets.text("schema", "Enter schema name")
dbutils.widgets.text("table", "Enter table name")

// COMMAND ----------

val schema = dbutils.widgets.get("schema")
val table = dbutils.widgets.get("table")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Check table structure

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE TABLE $schema.$table

// COMMAND ----------

// MAGIC %md
// MAGIC #### Check DELTA TABLE history

// COMMAND ----------

import io.delta.tables._

val deltaTable = DeltaTable.forName(s"$schema.$table")
val fullHistoryDF = deltaTable.history() 
display(fullHistoryDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Determine versions for each ingestion method. Ideally, identify a condition to do so automatically

// COMMAND ----------

import org.apache.spark.sql.functions.desc

// In this case, the condition to determine the newer ingestion is a WRITE operation with a null userName
var currentMethodVer = fullHistoryDF.filter($"userName".isNotNull && $"operation" === "WRITE").select($"version").orderBy(desc("version")).head().get(0).asInstanceOf[Long]
var newMethodVer = fullHistoryDF.filter($"userName".isNull && $"operation" === "WRITE").select($"version").orderBy(desc("version")).head().get(0).asInstanceOf[Long]

// COMMAND ----------

// MAGIC %md
// MAGIC #### Uncomment this code to force the versions

// COMMAND ----------

// currentMethodVer = ...
// newMethodVer = 77

// COMMAND ----------

spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW newMethod AS SELECT * FROM $schema.$table VERSION AS OF $newMethodVer")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create temporary views for each of the ingestion methods

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM newMethod LIMIT 100

// COMMAND ----------

spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW currentMethod AS SELECT * FROM $schema.$table VERSION AS OF $currentMethodVer")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM currentMethod LIMIT 100

// COMMAND ----------

val currentMethod_df = spark.table("currentMethod")
val newMethod_df = spark.table("newMethod")

// COMMAND ----------

// MAGIC %md
// MAGIC # In this section, a set of different checkings is applied

// COMMAND ----------

// MAGIC %md
// MAGIC ## 0.- Warning!! Detection of conflictive characters

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when, exists, max, instr}

val specificChar = "\\" // The character '\' is identified as a conflictive one
val conflictiveCharLookup = currentMethod_df.select(currentMethod_df.columns.map {
  columnName => when(instr(col(columnName), specificChar) > 0, true).otherwise(false).alias(columnName)
}: _*)

// COMMAND ----------

println(s"The following columns CONTAINS the conflictive char: '$specificChar'")
println("--------------------------------------------------------")
val final_map = conflictiveCharLookup.columns.map {
  columnName => (columnName, conflictiveCharLookup.agg(max(col(columnName))).head().getBoolean(0))
}

if (!final_map.exists(_._2)) {
  println(s"NO COLUMNS CONTAINING THE CONFLITIVE CHAR: '$specificChar' FOUND")
}

final_map.foreach { // Will show the column that contains the conflictive character
  dup => if (dup._2) {
    println(s"--> ${dup._1} <--")
  }
} 

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.- Check schemas

// COMMAND ----------

val currentMethodSchema = currentMethod_df.schema
val newMethodSchema = newMethod_df.schema

if (currentMethodSchema == newMethodSchema) {
    println("Current Method and New Method schemas ARE THE SAME\n")
} else {
    println("Current Method and New Method schemas ARE NOT THE SAME\n")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.- Check duplicates

// COMMAND ----------

// Duplicates current method

val currentMethod_df = spark.table("currentMethod")
val currentMethod_df_dedup = currentMethod_df.dropDuplicates()
if (currentMethod_df.count() != currentMethod_df_dedup.count()) {
  printf("There are DUPLICATES in the Current Method ingestion\n")
} else {
  printf("There are NOT DUPLICATES in the Current Method ingestion\n")
}

// COMMAND ----------

// Duplicates new method

val newMethod_df = spark.table("newMethod")
val newMethod_df_dedup = newMethod_df.dropDuplicates()
if (newMethod_df.count() != newMethod_df_dedup.count()) {
  printf("There are DUPLICATES in the New Method ingestion\n")
} else {
  printf("There are NOT DUPLICATES in the New Method ingestion\n")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3.- Check differences

// COMMAND ----------

val diffs_currentMethod_not_newMethod = currentMethod_df.exceptAll(newMethod_df)
val count_currentMethod_newMethod = diffs_currentMethod_not_newMethod.count()
println(s"Number of rows in which are in Current Method but not in New Method: $count_currentMethod_newMethod\n")

// COMMAND ----------

display(diffs_currentMethod_not_newMethod)

// COMMAND ----------

val diffs_newMethod_not_currentMethod = newMethod_df.exceptAll(currentMethod_df)
val count_newMethod_currentMethod = diffs_newMethod_not_currentMethod.count()
println(s"Number of rows in which are in New Method but not in Current Method: $count_newMethod_currentMethod\n")

// COMMAND ----------

display(diffs_newMethod_not_currentMethod)

// COMMAND ----------

if (count_newMethod_currentMethod == 0 && count_currentMethod_newMethod == 0) {
  printf("THERE ARE NOT DIFFERENCES !!!\n")
} else {
  printf("THERE ARE DIFFERENCES !!!\n")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Use this space to do your custom checkings
