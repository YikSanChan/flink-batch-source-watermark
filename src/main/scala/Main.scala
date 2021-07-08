import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row

object Main {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance.build
    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(execEnv, settings)
    tableEnv.executeSql(
      """
        |CREATE TABLE csv_source (
        |  a int,
        |  b int
        |) WITH (
        |  'connector' = 'filesystem',
        |  'format' = 'csv',
        |  'path' = '/tmp/input'
        |)""".stripMargin)
    tableEnv.executeSql(
      """
        |CREATE TABLE print_sink (
        |  a int
        |) WITH (
        |  'connector' = 'print'
        |)
        |""".stripMargin)
    val table = tableEnv.sqlQuery("SELECT a FROM csv_source")
    tableEnv.toDataStream(table).addSink(new SinkFunction[Row] {
      override def invoke(value: Row, context: SinkFunction.Context): Unit = {
        println(s"watermark=${context.currentWatermark()}")
        println(value.getField("a"))
      }
    })
    execEnv.execute()
  }
}