//1. Comienza una simple sesión Spark.
//mike@mike-HP-Notebook:~$/usr/local/spark-2.3.3-bin-hadoop2.7/bin/spark-shell

//2. Cargue el archivo Netflix Stock CSV, haga que Spark infiera los tipos de datos.

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema","true")csv("BIG-DATA/BigData-master/Spark_DataFrame/Netflix_2011_2016.csv")
//4. ¿Cómo es el esquema?
df.printSchema()

//3. ¿Cuáles son los nombres de las columnas?
df.columns

//5. Imprime las primeras 5 columnas.
df.select($"Date",$"Open", $"High", $"Low", $"Close").show()

//6. Usa describe () para aprender sobre el DataFrame.
df.describe("Open").show()
df.describe("Open", "Close").show()

//7. Crea un nuevo dataframe con una columna nueva llamada “HV Ratio” que es la relación entre
//el precio de la columna “High” frente a la columna “Volumen” de acciones negociadas por un
//día.
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.filter("Date == '2011-10-25' ").show()

//8. ¿Qué día tuvo el pico más alto en la columna “Close”?
df.select($"Date",$"High").sort($"High".desc).show(1)
println("El dia con pico mas alto en la columna High es: 13-07-2015 " )

//9. ¿Cuál es el significado de la columna Cerrar “Close”?
df.show(3)
println("La columna Close muestra la cantidad con la que se cierran los valores en ese dia")

//10. ¿Cuál es el máximo y mínimo de la columna “Volumen”?
df.select(max("Volume"), min("Volume")).show()

//11. Con Sintaxis Scala/Spark $ conteste los siguiente:
//a. ¿Cuántos días fue la columna “Close” inferior a $ 600?
val resultado = df.filter($"Close"< 600).count()

//b. ¿Qué porcentaje del tiempo fue la columna “High” mayor que $ 500?
val porcentaje: Double = ((df.filter($"High">500).count())*100.0)/df.count()
printf("El porcentaje es: %1.2f", porcentaje, "%")
println(" %")

//c. ¿Cuál es la correlación de Pearson entre columna “High” y la columna “Volumen”?
df.select(corr("High", "Volume")).show()

//d. ¿Cuál es el máximo de la columna “High” por año?
df.groupBy(year($"Date")).max("High").sort(year($"Date").desc).show()

//e. ¿Cuál es el promedio de columna “Close” para cada mes del calendario?
val df2 = df.withColumn("Month", month(df("Date")))
val dfavgs = df2.groupBy("Month").mean()
dfavgs.select($"Month", $"avg(Close)").sort("Month").show()
