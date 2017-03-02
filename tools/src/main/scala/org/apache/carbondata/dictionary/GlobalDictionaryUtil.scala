package org.apache.carbondata.dictionary

import java.util

import scala.collection.mutable.HashSet

import org.apache.spark.sql.Row

import org.apache.carbondata.CardinalityMatrix
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriterImpl, CarbonDictionarySortInfoPreparator}

/**
 * Created by knoldus on 1/3/17.
 */
class GlobalDictionaryUtil {

  def writeDictionary(carbonTable: CarbonTable,
      cardinalityMatrix: List[CardinalityMatrix],
      absoluteTableIdentifier: AbsoluteTableIdentifier) = {

    val dims = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
    val measures = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
    // val allColumns = List(dims, measures)
    val dimArrSet = Array[HashSet[String]]()
    var index = 0
    cardinalityMatrix.map { cardMatrix =>
      if (isDictionaryColumn(cardMatrix.cardinality)) {
        cardMatrix.columnDataframe.collect().map { (elem: Row) =>
          val data: String = elem.getAs[String](0)
          dimArrSet(index).add(data)
        }
        index += 1
      }
    }

    val dictCache: Cache[java.lang.Object, Dictionary] = CacheProvider.getInstance()
      .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath)
    var i = 0
    dimArrSet.map { dimSet =>
      val columnIdentifier = new ColumnIdentifier(dims.get(i).getColumnId, null, null)
      val writer = new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath,
        absoluteTableIdentifier.getCarbonTableIdentifier,
        columnIdentifier)

      dimSet.map(elem => writer.write(elem))
      writer.close()
      writer.commit()

      val dict: Dictionary = dictCache
        .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier,
          columnIdentifier, dims.get(i).getDataType))
      val newDistinctValues = new util.ArrayList[String]
      val dictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator()
      val carbonDictionarySortInfo = dictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dict, dims.get(i).getDataType)

      val carbonDictionarySortIndexWriter = new CarbonDictionarySortIndexWriterImpl(
        absoluteTableIdentifier.getCarbonTableIdentifier,
        columnIdentifier,
        absoluteTableIdentifier.getStorePath)
      i += 1

      try {
        carbonDictionarySortIndexWriter.writeSortIndex(carbonDictionarySortInfo.getSortIndex())
        carbonDictionarySortIndexWriter
          .writeInvertedSortIndex(carbonDictionarySortInfo.getSortIndexInverted())
      } finally {
        carbonDictionarySortIndexWriter.close()
      }
    }
  }

  def isDictionaryColumn(cardinality: Double): Boolean = {
    val cardinalityThreshold = 0.8
    if(cardinality > cardinalityThreshold) false
    else true
  }
}
