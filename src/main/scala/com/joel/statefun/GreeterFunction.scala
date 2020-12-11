package com.joel.statefun

import com.joel.statefun.GreeterFunction.{REQUEST_EGRESS, greetText}
import com.joel.statefun.messages.{InputMessage, OutputMessage}
import org.apache.flink.statefun.sdk.annotations.Persisted
import org.apache.flink.statefun.sdk.io.{EgressIdentifier, IngressIdentifier}
import org.apache.flink.statefun.sdk.state.PersistedValue
import org.apache.flink.statefun.sdk.{Context, FunctionType, StatefulFunction}

class GreeterFunction extends StatefulFunction {

  @Persisted
  final val seenCount = PersistedValue.of("seen-count", classOf[Int])

  override def invoke(context: Context, input: Any): Unit = {
    val userName = input.asInstanceOf[String]
    val outputMessage = computeOutputMessage(userName)
    context.send(REQUEST_EGRESS, outputMessage)
  }

  private def computeOutputMessage(userName: String): OutputMessage = {
    val seen = seenCount.updateAndGet(x => x + 1)
    val greeting = greetText(userName, seen)
    OutputMessage(greeting)
  }

}

object GreeterFunction {

  final val TYPE = new FunctionType("apache", "greeter")
  final val REQUEST_EGRESS = new EgressIdentifier[OutputMessage]("com.joel.stateful", "out", classOf[OutputMessage])

  private def greetText(name: String, seen: Int) = seen match {
    case 0 =>
      String.format("Hello %s ! \uD83D\uDE0E", name)
    case 1 =>
      String.format("Hello again %s ! \uD83E\uDD17", name)
    case 2 =>
      String.format("Third time is a charm! %s! \uD83E\uDD73", name)
    case 3 =>
      String.format("Happy to see you once again %s ! \uD83D\uDE32", name)
    case _ =>
      String.format("Hello at the x-th time %s \uD83D\uDE4C", name)
  }
}
