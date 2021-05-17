name := "kafka"

version := "0.1"

scalaVersion := "2.12.13"


//val sparkVersion = "2.3.1"
//val sparkVersion = "2.2.2"
val sparkVersion = "2.4.0"


lazy val root = (project in file(".")).
  settings(
    name := "kafka",
    version := "1.0",
    scalaVersion := "2.12.13",
    mainClass in Compile := Some("kafka")
  )

resolvers += Resolver.url("Typesafe Ivy releases", url("https://repo.typesafe.com/typesafe/ivy-releases"))(Resolver.ivyStylePatterns)

//libraryDependencies ++= Seq(
//  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
//  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided",
//  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
//  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
//  "com.typesafe" % "config" % "1.3.4"
//)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.10" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1" % Test,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" %  "3.0.2" % "provided",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.2" % Test,
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp")),
  "com.typesafe" % "config" % "1.3.4"
)

//libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.2"
//libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
//libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}