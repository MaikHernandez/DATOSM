//Libreria VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler

//Empieza la limpieza de los datos del csv "bank-full"
//Se carga Bank, el dataframe
val datosBank = spark.read.option("header","true").option("inferSchema","true").format("csv").load("BIG-DATA/BigData-master/Spark_LogisticRegression/bank-full.csv")
datosBank.show() //Muestra tabla 

val datosBank = spark.read.option("header","true").option("inferSchema", "true").option("delimiter",";").format("csv").load("BIG-DATA/BigData-master/Spark_LogisticRegression/bank-full.csv")

datosBank.show() //Muestra tabla 

datosBank.printSchema()

val cambio1 = datosBank.withColumn("y",when(col("y").equalTo("yes"),1).otherwise(col("y")))
val cambio2 = cambio1.withColumn("y",when(col("y").equalTo("no"),2).otherwise(col("y")))
//Ahora la columna sigue funcionando como un string 
val nuevacolum = cambio2.withColumn("y",'y.cast("Int"))
nuevacolum.show() //Muestra 1

//Arreglo de las caracteristicas con VectorAssembler
val assembler = (new VectorAssembler().setInputCols(Array("balance","day","duration","pdays","previous")).setOutputCol("features"))
val Ldata = assembler.transform(nuevacolum)
Ldata.show(1) //Muestra 1

//Se cambia el nombre de columna por "y"
val cambio = Ldata.withColumnRenamed("y", "label") // Se renombra la columna
val feat = cambio.select("label","features") 

feat.show() //Muestra el dataframe limpio


///// - Logistic Regresion
//Libreria LogisticRegression
import org.apache.spark.ml.classification.LogisticRegression

//Se crea un nuevo objeto de  LogisticRegression llamado lr
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

//Entrenamos el modelo
val lrModel = lr.fit(feat)

//Imprima los coeficientes e intercepte para logistic regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//También podemos utilizar la familia multinomial para la clasificación binaria.
val mlr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")
val mlrModel = mlr.fit(feat)

//Imprima los coeficientes y las intercepciones para regresión logística con familia multinomial
println(s"Multinomial Coeficientes: ${mlrModel.coefficientMatrix}") //Coeficientes
println(s"Multinomial Intercepciones: ${mlrModel.interceptVector}") //Intercepciones