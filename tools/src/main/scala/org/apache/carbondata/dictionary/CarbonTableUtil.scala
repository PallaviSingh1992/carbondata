package org.apache.carbondata.dictionary

import java.util.UUID

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.SchemaEvolution
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata,
CarbonTableIdentifier}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.SchemaEvolutionEntry

/**
 * Created by knoldus on 28/2/17.
 */
object CarbonTableUtil {

  def createDictionary(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): Array[Unit] = {
    val (carbonTable, absoluteTableIdentifier) = createCarbonTableMeta(cardinalityMatrix, dataFrame)
    val globalDictionaryUtil = new GlobalDictionaryUtil()
    globalDictionaryUtil.writeDictionary(carbonTable, cardinalityMatrix, absoluteTableIdentifier)

  }

  def createCarbonTableMeta(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): (CarbonTable, AbsoluteTableIdentifier) = {
    val tableInfo = new TableInfo()
    tableInfo.setStorePath("./target/store/T1")
    tableInfo.setDatabaseName("Default_DB")
    val tableSchema = new TableSchema()
    tableSchema.setTableName("Default_TB")
    import collection.JavaConversions._
    val encoding = List(Encoding.DICTIONARY)
    val absoluteTableIdentifier = new AbsoluteTableIdentifier("./target/store/T1",
      new CarbonTableIdentifier("Default_DB", "Default_TB", UUID.randomUUID().toString()))
    var columnGroupId = -1
    val columnSchemas: List[ColumnSchema] = cardinalityMatrix.map { element =>
      val columnSchema = new ColumnSchema()
      columnSchema.setColumnName(element.columnName)
      columnSchema.setColumnar(true)
      columnSchema.setDataType(parseDataType(element.dataType))
      columnSchema.setEncodingList(encoding)
      columnSchema.setColumnUniqueId(element.columnName)
      columnSchema
        .setDimensionColumn(checkDimensionColumn(columnSchema.getDataType, element.cardinality))
      // TODO: assign column group id to all columns
      columnGroupId += 1
      columnSchema.setColumnGroup(columnGroupId)
      columnSchema
    }
    tableSchema.setListOfColumns(columnSchemas)
    val schemaEvol = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(List())
    tableSchema.setSchemaEvalution(schemaEvol)
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableInfo.setTableUniqueName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName + "_" +
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setAggregateTableList(List())
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(absoluteTableIdentifier.getStorePath,
        absoluteTableIdentifier.getCarbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath()
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo,
        tableInfo.getDatabaseName,
        tableInfo.getFactTable().getTableName)

    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime())

    val schemaEvolutionEntries = thriftTableInfo.getFact_table().getSchema_evolution()
      .getSchema_evolution_history().add(schemaEvolutionEntry)

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }

    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    (CarbonMetadata.getInstance()
      .getCarbonTable(tableInfo.getTableUniqueName()), absoluteTableIdentifier)
  }

  import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}

  def parseDataType(dataType: DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.STRING
      case FloatType => CarbonDataType.FLOAT
      case IntegerType => CarbonDataType.INT
      case ByteType => CarbonDataType.SHORT
      case ShortType => CarbonDataType.SHORT
      case DoubleType => CarbonDataType.DOUBLE
      case LongType => CarbonDataType.LONG
      case BooleanType => CarbonDataType.BOOLEAN
      case DateType => CarbonDataType.DATE
      case DecimalType.USER_DEFAULT => CarbonDataType.DECIMAL
      case TimestampType => CarbonDataType.TIMESTAMP
      case _ => CarbonDataType.STRING
    }
  }

  def checkDimensionColumn(carbonDataType: CarbonDataType, cardinality: Double): Boolean = {
    val cardinalityThreshold = 0.8
    //TODO: Columns for which dictionary will be created are considered as dimension columns
    if (cardinality > cardinalityThreshold) {
      false
    } else {
      true
    }
  }

}
