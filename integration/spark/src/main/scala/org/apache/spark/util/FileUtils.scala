/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import org.apache.spark.Logging

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.processing.etl.DataLoadingException

object FileUtils extends Logging {
  /**
   * append all csv file path to a String, file path separated by comma
   */
  private def getPathsFromCarbonFile(carbonFile: CarbonFile, stringBuild: StringBuilder): Unit = {
    carbonFile.isDirectory match {
    case true =>
      val files = carbonFile.listFiles()
      for (j <- 0 until files.size) {
        getPathsFromCarbonFile(files(j), stringBuild)
      }
    case false =>
      val path = carbonFile.getAbsolutePath
      val fileName = carbonFile.getName
      if (carbonFile.getSize == 0) {
        logWarning(s"skip empty input file: $path")
      } else if (fileName.startsWith(CarbonCommonConstants.UNDERSCORE) ||
          fileName.startsWith(CarbonCommonConstants.POINT)) {
        logWarning(s"skip invisible input file: $path")
      } else if (fileName.toLowerCase().endsWith(".csv")) {
        stringBuild.append(path.replace('\\', '/')).append(CarbonCommonConstants.COMMA)
      } else {
        logWarning(s"skip input file: $path, because this path doesn't end with '.csv'")
      }
    }
  }

  /**
   * append all file path to a String, inputPath path separated by comma
   *
   */
  def getPaths(inputPath: String): String = {
    if (inputPath == null || inputPath.isEmpty) {
      throw new DataLoadingException("input file path cannot be empty.")
    } else {
      val stringBuild = new StringBuilder()
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        if (!carbonFile.exists()) {
          throw new DataLoadingException(s"the input file does not exist: ${filePaths(i)}" )
        }
        getPathsFromCarbonFile(carbonFile, stringBuild)
      }
      if (stringBuild.nonEmpty) {
        stringBuild.substring(0, stringBuild.size - 1)
      } else {
        throw new DataLoadingException("please check your input path and make sure " +
          "that files end with '.csv' and content is not empty.")
      }
    }
  }

  def getSpaceOccupied(inputPath: String): Long = {
    var size : Long = 0
    if (inputPath == null || inputPath.isEmpty) {
      size
    } else {
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        size = size + carbonFile.getSize
      }
      size
    }
  }
}
