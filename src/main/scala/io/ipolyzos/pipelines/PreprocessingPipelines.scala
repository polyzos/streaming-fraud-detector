package io.ipolyzos.pipelines

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler, VectorSlicer}
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

import scala.collection.mutable

object PreprocessingPipelines {
  def createStringIndexer(columns: List[String]): List[StringIndexer] = {
    columns.map { column =>
      new StringIndexer()
        .setInputCol(column)
        .setOutputCol(s"${column}_indexed")
    }
  }

  def createOneHotEncoder(columns: List[String]): List[OneHotEncoderEstimator] = {
    columns.map { column =>
      new OneHotEncoderEstimator()
        .setInputCols(Array(s"${column}_indexed"))
        .setInputCols(Array(s"${column}_encoded"))
    }
  }

  def createVectorAssmbler(featureColumns: List[String]): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol("features")
  }

  def createFeatureExtractionPipeline(schema: StructType, columns: List[String]): Array[PipelineStage] = {
    val featureColumns = mutable.ArrayBuffer[String]()
    val scaleFeatureColumns = mutable.ArrayBuffer[String]()

    val preprocessingStages = schema.fields
      .filter { field =>
        columns.contains(field.name)
      }.flatMap { field =>
      field.dataType match {
        case _: StringType => val stringIndexer = new StringIndexer()
          .setInputCol(field.name)
          .setOutputCol(s"${field.name}_indexed")
          Array[PipelineStage](stringIndexer)

        case _: NumericType =>
          scaleFeatureColumns += (field.name)
          Array.empty[PipelineStage]

        case _ => Array.empty[PipelineStage]
      }
    }

    val numericAssembler = new VectorAssembler()
      .setInputCols(scaleFeatureColumns.toArray)
      .setOutputCol("numericRawFeatures")

    val slicer = new VectorSlicer()
      .setInputCol("numericRawFeatures")
      .setOutputCol("slicedfeatures")
      .setNames(scaleFeatureColumns.toArray)

    val scaler = new StandardScaler()
      .setInputCol("slicedfeatures")
      .setOutputCol("scaledfeatures")
      .setWithStd(true)
      .setWithMean(true)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray :+ "scaledfeatures")
      .setOutputCol("features")

    (preprocessingStages ++ Array(numericAssembler, slicer, scaler, vectorAssembler))
  }

  def createStringIndexerPipeline(schema: StructType, columns: List[String]): Array[PipelineStage] = {
    val featureColumns = mutable.ArrayBuffer[String]()
    val preprocessingStages = schema.fields.filter { field =>
      columns.contains(field.name)
    }.flatMap { field =>

      field.dataType match {
        case _: StringType =>

          val stringIndexer = new StringIndexer()
            .setInputCol(field.name)
            .setOutputCol(s"${field.name}_indexed")

          featureColumns += (s"${field.name}_indexed")
          Array[PipelineStage](stringIndexer)

        case _: NumericType =>
          featureColumns += (field.name)
          Array.empty[PipelineStage]

        case _ => Array.empty[PipelineStage]

      }
    }
    val vectorAssmbler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol("features")

    (preprocessingStages :+ vectorAssmbler)
  }
}
