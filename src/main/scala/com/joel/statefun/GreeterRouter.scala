package com.joel.statefun

import com.joel.statefun.messages.InputMessage
import org.apache.flink.statefun.sdk.io.Router

object GreeterRouter extends Router[InputMessage] {

  override def route(message: InputMessage, downstream: Router.Downstream[InputMessage]): Unit = {
    downstream.forward(GreeterFunction.TYPE, message.userName, message)
  }
}
