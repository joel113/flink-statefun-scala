package com.joel.statefun

import org.apache.flink.statefun.flink.datastream.SerializableStatefulFunctionProvider
import org.apache.flink.statefun.sdk.{FunctionType, StatefulFunction}

class GreeterFunctionProvider extends SerializableStatefulFunctionProvider {

  override def functionOfType(functionType: FunctionType): StatefulFunction = {
    new GreeterFunction()
  }
}
