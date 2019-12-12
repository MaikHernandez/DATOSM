//LIbrerias
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.tree.model
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator 
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


///// - Decision Three
//Librerias que se utilizan
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.tree.model
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator 

//Indice etiquetas, agregando datos a la columna de etiquetas.
//Ajustar en el conjunto de datos completo para incluir todas las etiquetas en el índice.
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(feat)

//Identificar automáticamente las características categóricas, e indexarlas.
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4) // features with > 4 distinct values are treated as continuous.  .fit(data)

//Dividir los datos en entrenamiento y prueba 
val Array(trainingData, testData) = feat.randomSplit(Array(0.7, 0.3))

//Entrenamos con modelo de DecisionTree.
val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

//Convertir etiquetas indexadas de nuevo a etiquetas originales.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

//Se crea el pipeline, agrupando los datos
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

//Se junta el modelo y lo entrena 
val model = pipeline.fit(trainingData)

//Predicciones
val predictions = model.transform(testData)

//Se muestran filas de ejemplo (5)
predictions.select("predictedLabel", "label", "features").show(5)

//Seleccione (predicción, etiqueta verdadera) y calcule el error de prueba
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)

println(s"Test Error = ${(1.0 - accuracy)}") //Error de prueba

val arbolModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println(s"Learned classification tree model:\n ${arbolModel.toDebugString}")