Common.settings("examples")

import Common.dependencies._

libraryDependencies ++= pekko ++ scalikejdbc ++ postgresql

// Common.dependencies.logback is Test-scoped; the example has a runnable main, so it needs a
// logging backend at runtime too.
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.32" % Runtime

// Documentation, not a published artifact.
publish / skip := true
