/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.example

import com.twitter.algebird.Monoid
import com.twitter.bijection._
import com.twitter.bijection.netty.Implicits._
import com.twitter.conversions.time._
import com.twitter.storehaus.Store
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.BatchID
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.exp.mysql.{ Client}
import com.twitter.storehaus.mysql._


import com.twitter.bijection.{ Injection }
import scala.util.Try
import org.jboss.netty.util.CharsetUtil._
import com.twitter.finagle.exp.mysql.RawStringValue
import scala.Array



object Mysql {
  val DEFAULT_TIMEOUT = 1.seconds

  def client = Client("localhost:3306", "root", "root", "storehaus_test")

  /**
   * Returns a function that encodes a key to a Mysql key string
   * given a unique namespace string.
   */
  def keyEncoder[T](namespace: String)
                   (implicit inj: Codec[T]): T =>   MySqlValue  = { key: T =>
    MySqlValue(namespace)
  }

  def store[K: Codec, V: Codec](keyPrefix: String): Store[K, V] = {
    implicit val mysqlbig = MySqlCbInjection2
    implicit val valueToBuf =  Injection.connect[V,  Array[Byte], ChannelBuffer, MySqlValue]
    MySqlStore(client, "storehaus-mysql-test", "key", "value").convert(keyEncoder[K](keyPrefix))
  }

  def mergeable[K: Codec, V: Codec: Monoid](keyPrefix: String): MergeableStore[K, V] =
    MergeableStore.fromStore(store[K, V](keyPrefix))
}

object MySqlCbInjection2 extends Injection[ChannelBuffer,MySqlValue] {
  def apply(a: ChannelBuffer): MySqlValue = MySqlValue(RawStringValue(a.toString(UTF_8)))
  override def invert(b: MySqlValue) = Try(ValueMapper.toChannelBuffer(b.v).getOrElse(ChannelBuffers.EMPTY_BUFFER))
}

