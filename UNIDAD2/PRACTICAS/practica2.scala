//Decision Tree

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

// Se cargan los datos en la variable data
val data = spark.read.format("libsvm").load("BIG-DATA/BigData-master/data/sample_libsvm_data.txt")

// se agrega una columna label y se ajustan los datos 
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

// Se declarara un array con el 70% de datos para entrenamiento y el 30% para pruebas.
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Se declara el Clasificador de árbol de decisión con las caracteristicas que se van a evaluar
val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

//Se agrega la etiqueta de prediccion
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)


val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

// Se entrena el modelo con los datos de entrenamiento
val model = pipeline.fit(trainingData)

// Se hacen las predicciones con los datos de pruebas
val predictions = model.transform(testData)

// Se manda a imprimir las etiquetas y las predicciones
predictions.select("predictedLabel", "label", "features").show(5)

// Evalua la precision de las predicciones
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
// Se calcula la precision en base a las predicciones
val accuracy = evaluator.evaluate(predictions)
// Se manda a imprimir el resultado de error 
println(s"Test Error = ${(1.0 - accuracy)}")

// Se genera el modelo de arbol
val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

println(s"Learned classification tree model:\n ${treeModel.toDebugString}")