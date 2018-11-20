/*
 * Copyright (c) 2018 Adobe Systems Inc. All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the
 * property of Adobe Systems Incorporated and its suppliers, if any.
 * The intellectual and technical concepts contained herein are
 * proprietary to Adobe Systems Incorporated and its suppliers and may
 * be covered by U.S. and Foreign Patents, patents in process, and are
 * protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Adobe Systems Inc.
 */

package jmh.main

import com.typesafe.config.{Config, ConfigFactory}

package object config {
  val config: Config = ConfigFactory.load().resolve()

  val cosmosSettings = CosmosSettings(config)

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalInt(path: String): Option[Int] = if (underlying.hasPath(path)) {
      Some(underlying.getInt(path))
    } else {
      None
    }

    def getOptionalString(path: String): Option[String] = if (underlying.hasPath(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }

    def getIntOrElse(path: String, default: Int): Int = if (underlying.hasPath(path)) {
      underlying.getInt(path)
    } else {
      default
    }

    def getStringOrElse(path: String, default: String): String = if (underlying.hasPath(path)) {
      underlying.getString(path)
    } else {
      default
    }
  }
}
