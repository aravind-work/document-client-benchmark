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

import jmh.main.BatchQueryBenchmark
import org.slf4j.{Logger, LoggerFactory}

/**
  * ONLY USE TO DEBUG IN IDE
  */
object DebugBenchmarkInIDE {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val iterations = 100

    val myBenchmark = new BatchQueryBenchmark()

    myBenchmark.setupTrial()

    1 to iterations foreach { _ =>
      myBenchmark.batchQueryBenchmark()
    }
  }

}