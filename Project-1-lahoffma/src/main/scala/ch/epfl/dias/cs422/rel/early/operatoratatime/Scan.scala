package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  private def getColumns: IndexedSeq[Column] = {
    val colBuffer = ListBuffer[Column]()
    for (i <- 0 until getRowType.getFieldCount) {
      val col: Column = unwrap(scannable.getColumn(i)).toIndexedSeq
      colBuffer += col
    }
    colBuffer.toIndexedSeq
  }

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[Column] = {
    val columns = getColumns
    val trueCol = for (_ <- 0L until scannable.getRowCount) yield true
    columns :+ trueCol
  }
}
