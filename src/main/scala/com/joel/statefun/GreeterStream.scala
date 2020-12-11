package com.joel.statefun

import com.joel.statefun.GreeterFunction.REQUEST_EGRESS
import com.joel.statefun.messages.{InputMessage, OutputMessage}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig
import org.apache.flink.statefun.flink.core.message.MessageFactoryType
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction

object GreeterStream {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val statefunConfig = StatefulFunctionsConfig.fromEnvironment(streamExecutionEnvironment)
    statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS)

    val greeterStream = streamExecutionEnvironment
      .socketTextStream("localhost", 9000)
      .map(userName =>
        RoutableMessageBuilder.builder()
          .withTargetAddress(GreeterFunction.TYPE, userName)
          .withMessageBody(userName)
          .build())

    val statefulGreeterStream = StatefulFunctionDataStreamBuilder.builder("example")
      .withDataStreamAsIngress(greeterStream)
      .withFunctionProvider(GreeterFunction.TYPE, new GreeterFunctionProvider())
      .withEgressId(REQUEST_EGRESS)
      .withConfiguration(statefunConfig)
      .build(streamExecutionEnvironment)

    val output = statefulGreeterStream.getDataStreamForEgressId(REQUEST_EGRESS)

    output
      .map(
        new RichMapFunction[OutputMessage, String]() {
          override def map(outputMessage: OutputMessage): String = "Message: " + outputMessage.message
        })
      .addSink(new PrintSinkFunction[String])

    streamExecutionEnvironment.execute("Stateful Greeter Example")

  }

}
