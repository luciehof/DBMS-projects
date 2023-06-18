package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, RLEColumn, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var tuples = IndexedSeq[Tuple]()

  /**
    * Helper function (you do not have to use it or implement it)
    * It's purpose is to show how to convert the [[scannable]] to a
    * specific [[Store]].
    *
    * @param rowId row number (startign from 0)
    * @return the row as a Tuple
    */
  private def getRow(rowId: Int): Tuple = {
    scannable match {
      case rleStore: RLEStore =>
        /**
          * For this project, it's safe to assume scannable will always
          * be a [[RLEStore]].
          */
        if (tuples.isEmpty) {
          val cols = ListBuffer[Tuple]()
          for (i <- 0 until getRowType.getFieldCount) {
            val col = ListBuffer[Elem]()
            rleStore
              .getRLEColumn(i)
              .foreach(e => {
                var l = 0
                while (l < e.length) {
                  col += e.value(0)
                  l += 1
                }
              })
            cols += col.toIndexedSeq
          }
          tuples = cols.toIndexedSeq.transpose
          tuples(rowId)
        } else {
          tuples(rowId)
        }

    }
  }

  private var nextId: Int = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    nextId = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (nextId < scannable.getRowCount) {
      val ret = Option(getRow(nextId))
      nextId += 1
      ret
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    nextId = -1
    tuples = IndexedSeq[RLEColumn]()
  }
}
