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
nuevacolum.show(1) //Muestra 1

//Arreglo de las caracteristicas con VectorAssembler
val assembler = (new VectorAssembler().setInputCols(Array("balance","day","duration","pdays","previous")).setOutputCol("features"))
val Ldata = assembler.transform(nuevacolum)
Ldata.show(1) //Muestra 1

//Se cambia el nombre de columna por "y"
val cambio = Ldata.withColumnRenamed("y", "label") // Se renombra la columna
val feat = cambio.select("label","features") 

feat.show() //Muestra el dataframe limpio



///// - Multilayer perceptron
//Librerias a utilizar 
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

//Datos de entrenamiento:70 y prueba:30
val splits = feat.randomSplit(Array(0.7, 0.3), seed = 1234L)
val train = splits(0)
val test = splits(1)

//Se especifica la red neuronal que se va a utilizar
//Capa entrada de tamaño 5 (features)
//dos Capas intermedias  de tamaño 2 y 2
//Capa salida 4
val capasN = Array[Int](5, 6, 3, 4)

//Se hace la creacion del entrenador y se le dan los parametros
//se esta usando el modelo del clasificador
val entrenador = new MultilayerPerceptronClassifier().setLayers(capasN).setBlockSize(128).setSeed(1234L).setMaxIter(100)

//Se junta el modelo y lo entrena
val model = entrenador.fit(train)

//Se crea la variable de los resultados de la prueba
val resultados = model.transform(test)
resultados.show()

//Prediciendo la exactitud con un evaluador
val predictionAndLabels = resultados.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

//Se imprime la exactitud
println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")