package org.apache.carbondata.dictionary

import java.util

import org.apache.spark.sql.Row

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, CarbonMeasure}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriterImpl,
CarbonDictionarySortInfoPreparator}

trait GlobalDictionaryUtil {

  def writeDictionary(carbonTable: CarbonTable,
      cardinalityMatrix: List[CardinalityMatrix],
      absoluteTableIdentifier: AbsoluteTableIdentifier): Unit = {

    val dimensions: util.List[CarbonDimension] = carbonTable
      .getDimensionByTableName(carbonTable.getFactTableName)
    //TODO List of Columns of type measures
    val measures: util.List[CarbonMeasure] = carbonTable
      .getMeasureByTableName(carbonTable.getFactTableName)
    val dimArrSet: Array[Set[String]] = identifyDictionaryColumns(cardinalityMatrix,
      dimensions)
    writeDictionaryToFile(absoluteTableIdentifier, dimArrSet, dimensions)
  }

  private def identifyDictionaryColumns(cardinalityMatrix: List[CardinalityMatrix],
      dimensions: util.List[CarbonDimension]): Array[Set[String]] = {
    val dimArrSet: Array[Set[String]] = new Array[Set[String]](dimensions.size())
    cardinalityMatrix.zipWithIndex.map { case (columnCardinality, index) =>
      if (isDictionaryColumn(columnCardinality.cardinality)) {
        dimArrSet(index) = Set[String]()
        columnCardinality.columnDataframe.collect().map { (elem: Row) =>
          val data: String = elem.get(0).toString
          dimArrSet(index) += data
        }
      }
    }
    dimArrSet
  }

  private def writeDictionaryToFile(absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimArrSet: Array[Set[String]],
      dimensions: util.List[CarbonDimension]): Unit = {
    val dictCache: Cache[java.lang.Object, Dictionary] = CacheProvider.getInstance()
      .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath)
    dimArrSet.zipWithIndex.foreach { case (dimSet, i) =>
      val columnIdentifier: ColumnIdentifier = new ColumnIdentifier(dimensions.get(i).getColumnId,
        null, null)
      val writer = dictionaryWriter(columnIdentifier, absoluteTableIdentifier, dimensions, i)

      dimSet.map(elem => writer.write(elem))
      writer.close()
      writer.commit()

      val dict: Dictionary = dictCache
        .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier,
          columnIdentifier, dimensions.get(i).getDataType))
      sortIndexWriter(dict, columnIdentifier, absoluteTableIdentifier, dimensions, i)
    }
  }

  private def dictionaryWriter(columnIdentifier: ColumnIdentifier,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimensions: util.List[CarbonDimension],
      index: Int): CarbonDictionaryWriterImpl = {
    val writer: CarbonDictionaryWriterImpl = new CarbonDictionaryWriterImpl(absoluteTableIdentifier
      .getStorePath,
      absoluteTableIdentifier.getCarbonTableIdentifier,
      columnIdentifier)
    writer
  }

  private def sortIndexWriter(dict: Dictionary,
      columnIdentifier: ColumnIdentifier,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimensions: util.List[CarbonDimension],
      index: Int): Unit = {
    val newDistinctValues = new util.ArrayList[String]
    val dictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator()
    val carbonDictionarySortInfo = dictionarySortInfoPreparator
      .getDictionarySortInfo(newDistinctValues, dict, dimensions.get(index).getDataType)

    val carbonDictionarySortIndexWriter = new CarbonDictionarySortIndexWriterImpl(
      absoluteTableIdentifier.getCarbonTableIdentifier,
      columnIdentifier,
      absoluteTableIdentifier.getStorePath)
    try {
      carbonDictionarySortIndexWriter.writeSortIndex(carbonDictionarySortInfo.getSortIndex())
      carbonDictionarySortIndexWriter
        .writeInvertedSortIndex(carbonDictionarySortInfo.getSortIndexInverted())
    } finally {
      carbonDictionarySortIndexWriter.close()
    }
  }

  def isDictionaryColumn(cardinality: Double): Boolean = {
    val cardinalityThreshold = 0.8
    if (cardinality > cardinalityThreshold) {
      false
    } else {
      true
    }
  }
}

object GlobalDictionaryUtil extends GlobalDictionaryUtil
