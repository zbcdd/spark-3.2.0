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

package org.apache.spark.mllib.fpm

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Calculate all patterns of a projected database in local mode.
 *
 * @param minCount minimal count for a frequent pattern
 * @param maxPatternLength max pattern length for a frequent pattern
 */
private[fpm] class LocalCSP(
    val DN: Double,
    val DA: Double,
    val minGR: Double,
    val minCount: Long,
    val maxPatternLength: Int) extends Logging with Serializable {
  import CSP.Postfix
  import LocalCSP.ReversedPrefix

  /**
   * Generates frequent patterns on the input array of postfixes.
   * @param postfixes an array of postfixes
   * @return an iterator of (frequent pattern, count)
   */
  def run(postfixesA: Array[Postfix],
          postfixesN: Array[Postfix]): Iterator[(Array[Int], Double, Long, Long)] = {
    genFreqPatterns(ReversedPrefix.empty,
                    postfixesA,
                    postfixesN).map {
      case (prefix, growthRate, countA, countN) =>
        (prefix.toSequence, growthRate, countA, countN)
    }
  }


  private def counts(postfixes: Array[Postfix]): mutable.Map[Int, Long] = {
    val counts = mutable.Map.empty[Int, Long].withDefaultValue(0)
    postfixes.foreach { postfix =>
      postfix.genPrefixItems.foreach { case (x, _) =>
        counts(x) += 1L
      }
    }
    counts
  }

  /**
   * Recursively generates frequent patterns.
   * @param prefix current prefix
   * @param postfixes projected postfixes w.r.t. the prefix
   * @return an iterator of (prefix, count)
   */
  private def genFreqPatterns(
      prefix: ReversedPrefix,
      postfixesA: Array[Postfix],
      postfixesN: Array[Postfix]): Iterator[(ReversedPrefix, Double, Long, Long)] = {
    if (maxPatternLength == prefix.length || postfixesA.length < minCount) {
      return Iterator.empty
    }
    // find frequent items
    val countsA = counts(postfixesA)
    val countsN = counts(postfixesN)
    val freqItems = countsA.toSeq.filter { case (_, count) =>
      count >= minCount
    }.sorted
    // project and recursively call genFreqPatterns
    freqItems.toIterator.flatMap { case (item, count) =>
      val newPrefix = prefix :+ item
      val countA = countsA(item)
      val countN = countsN(item)
      val GR = (countA.asInstanceOf[Double] / DA) / (countN.asInstanceOf[Double] / DN)
      val projectedA = postfixesA.map(_.project(item)).filter(_.nonEmpty)
      val projectedN = postfixesN.map(_.project(item)).filter(_.nonEmpty)
      val csps = genFreqPatterns(newPrefix, projectedA, projectedN)
      if (GR >= minGR) {
        Iterator.single((newPrefix, GR, countA, countN)) ++ csps
      } else {
        csps
      }
    }
  }
}

private object LocalCSP {

  /**
   * Represents a prefix stored as a list in reversed order.
   * @param items items in the prefix in reversed order
   * @param length length of the prefix, not counting delimiters
   */
  class ReversedPrefix private (val items: List[Int], val length: Int) extends Serializable {
    /**
     * Expands the prefix by one item.
     */
    def :+(item: Int): ReversedPrefix = {
      require(item != 0)
      if (item < 0) {
        new ReversedPrefix(-item :: items, length + 1)
      } else {
        new ReversedPrefix(item :: 0 :: items, length + 1)
      }
    }

    /**
     * Converts this prefix to a sequence.
     */
    def toSequence: Array[Int] = (0 :: items).toArray.reverse
  }

  object ReversedPrefix {
    /** An empty prefix. */
    val empty: ReversedPrefix = new ReversedPrefix(List.empty, 0)
  }
}
