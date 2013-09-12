import sbt._
import sbt.Keys._
import Tests._

object TestPolicy {

  def single(test: TestDefinition): Group =
    new Group(test.name, Seq(test), SubProcess(ForkOptions()))

  def forkedUnique = Seq(
    fork in Test := true,
    testGrouping := (definedTests in Test).value map single
  )
}
