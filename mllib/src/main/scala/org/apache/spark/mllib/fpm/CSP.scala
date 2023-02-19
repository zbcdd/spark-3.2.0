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

import java.{lang => jl, util => ju}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.CSP.Prefix.{cachePrefix, findCachedId}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * A parallel PrefixSpan algorithm to mine frequent sequential patterns.
 * The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan: Mining Sequential Patterns
 * Efficiently by Prefix-Projected Pattern Growth
 * (see <a href="https://doi.org/10.1109/ICDE.2001.914830">here</a>).
 *
 * @param minSupport the minimal support level of the sequential pattern, any pattern that appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 *                   注：minSupport只针对异常数据库。
 * @param maxPatternLength the maximal length of the sequential pattern
 * @param maxLocalProjDBSize The maximum number of items (including delimiters used in the internal
 *                           storage format) allowed in a projected database before local
 *                           processing. If a projected database exceeds this size, another
 *                           iteration of distributed prefix growth is run.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Sequential_Pattern_Mining">Sequential Pattern Mining
 * (Wikipedia)</a>
 */
@Since("1.5.0")
class CSP private (
    private var minGR: Double,
    private var minSupport: Double,
    private var maxPatternLength: Int,
    private var maxLocalProjDBSize: Long) extends Logging with Serializable {
  import CSP._

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`, maxLocalProjDBSize: `32000000L`}.
   */
  @Since("1.5.0")
  def this() = this(2.0, 0.1, 10, 32000000L)


  @Since("1.5.0")
  def getMinGR: Double = minGR


  @Since("1.5.0")
  def setMinGR(minGR: Double): this.type = {
    this.minGR = minGR
    this
  }

  /**
   * Get the minimal support (i.e. the frequency of occurrence before a pattern is considered
   * frequent).
   */
  @Since("1.5.0")
  def getMinSupport: Double = minSupport

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  @Since("1.5.0")
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1,
      s"The minimum support value must be in [0, 1], but got $minSupport.")
    this.minSupport = minSupport
    this
  }

  /**
   * Gets the maximal pattern length (i.e. the length of the longest sequential pattern to consider.
   */
  @Since("1.5.0")
  def getMaxPatternLength: Int = maxPatternLength

  /**
   * Sets maximal pattern length (default: `10`).
   */
  @Since("1.5.0")
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    // TODO: support unbounded pattern length when maxPatternLength = 0
    require(maxPatternLength >= 1,
      s"The maximum pattern length value must be greater than 0, but got $maxPatternLength.")
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Gets the maximum number of items allowed in a projected database before local processing.
   */
  @Since("1.5.0")
  def getMaxLocalProjDBSize: Long = maxLocalProjDBSize

  /**
   * Sets the maximum number of items (including delimiters used in the internal storage format)
   * allowed in a projected database before local processing (default: `32000000L`).
   */
  @Since("1.5.0")
  def setMaxLocalProjDBSize(maxLocalProjDBSize: Long): this.type = {
    require(maxLocalProjDBSize >= 0L,
      s"The maximum local projected database size must be nonnegative, but got $maxLocalProjDBSize")
    this.maxLocalProjDBSize = maxLocalProjDBSize
    this
  }

  /**
   * Finds the complete set of frequent sequential patterns in the input sequences of itemsets.
   * @param dataN sequences of itemsets (Normal).
   * @param dataA sequences of itemsets (Anomaly).
   * @return a [[CSPModel]] that contains the frequent patterns
   */
  @Since("1.5.0")
  def run[Item: ClassTag](
      dataN: RDD[Array[Array[Item]]],
      dataA: RDD[Array[Array[Item]]]): CSPModel[Item] = {
    if (dataN.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data (normal) is not cached.")
    }
    if (dataA.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data (anomaly) is not cached.")
    }

    val totalCountN = dataN.count()
    val totalCountA = dataA.count()
    logInfo(s"number of normal sequences: $totalCountN, number of anomaly sequences: $totalCountA")
    val minCount = math.ceil(minSupport * totalCountA).toLong
    logInfo(s"minimum count for a frequent pattern (only pay attention to anomaly): $minCount")

    // Find frequent items (only pay attention to anomaly database).
    val freqItems = findFrequentItems(dataA, minCount)
    logInfo(s"number of frequent items: ${freqItems.length}")

    // Keep only frequent items from input sequences and convert them to internal storage.
    val itemToInt = freqItems.zipWithIndex.toMap
    val dataInternalReprN = toDatabaseInternalRepr(dataN, itemToInt)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val dataInternalReprA = toDatabaseInternalRepr(dataA, itemToInt)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val results = genFreqPatterns(
      dataInternalReprN, dataInternalReprA,
      totalCountN, totalCountA,
      minGR, minCount,
      maxPatternLength, maxLocalProjDBSize)

    def toPublicRepr(pattern: Array[Int]): Array[Array[Item]] = {
      val sequenceBuilder = mutable.ArrayBuilder.make[Array[Item]]
      val itemsetBuilder = mutable.ArrayBuilder.make[Item]
      val n = pattern.length
      var i = 1
      while (i < n) {
        val x = pattern(i)
        if (x == 0) {
          sequenceBuilder += itemsetBuilder.result()
          itemsetBuilder.clear()
        } else {
          itemsetBuilder += freqItems(x - 1) // using 1-indexing in internal format
        }
        i += 1
      }
      sequenceBuilder.result()
    }

    val freqSequences = results.map {
      case (seq: Array[Int], growthRate: Double, countA: Long, countN: Long) =>
        new FreqSequence(toPublicRepr(seq), growthRate, countA, countN)
    }
    // Cache the final RDD to the same storage level as input
    if (dataN.getStorageLevel != StorageLevel.NONE) {
      freqSequences.persist(dataN.getStorageLevel)
      freqSequences.count()
    }
    dataInternalReprN.unpersist()
    dataInternalReprA.unpersist()

    new CSPModel(freqSequences)
  }

  /**
   * A Java-friendly version of `run()` that reads sequences from a `JavaRDD` and returns
   * frequent sequences in a [[CSPModel]].
   * @param data ordered sequences of itemsets stored as Java Iterable of Iterables
   * @tparam Item item type
   * @tparam Itemset itemset type, which is an Iterable of Items
   * @tparam Sequence sequence type, which is an Iterable of Itemsets
   * @return a [[CSPModel]] that contains the frequent sequential patterns
   */
  @Since("1.5.0")
  def run[Item, Itemset <: jl.Iterable[Item], Sequence <: jl.Iterable[Itemset]](
      dataN: JavaRDD[Sequence], dataA: JavaRDD[Sequence]): CSPModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(dataN.rdd.map(_.asScala.map(_.asScala.toArray).toArray),
      dataA.rdd.map(_.asScala.map(_.asScala.toArray).toArray))
  }

}

@Since("1.5.0")
object CSP extends Logging {

  /**
   * This methods finds all frequent items in a input dataset.
   *
   * @param data Sequences of itemsets.
   * @param minCount The minimal number of sequence an item should be present in to be frequent
   *
   * @return An array of Item containing only frequent items.
   */
  private[fpm] def findFrequentItems[Item: ClassTag](
      data: RDD[Array[Array[Item]]],
      minCount: Long): Array[Item] = {

    data.flatMap { itemsets =>
      val uniqItems = mutable.Set.empty[Item]
      itemsets.foreach(set => uniqItems ++= set)
      uniqItems.toIterator.map((_, 1L))
    }.reduceByKey(_ + _).filter { case (_, count) =>
      count >= minCount
    }.sortBy(-_._2).map(_._1).collect()
  }

  /**
   * This methods cleans the input dataset from un-frequent items, and translate it's item
   * to their corresponding Int identifier.
   *
   * @param data Sequences of itemsets.
   * @param itemToInt A map allowing translation of frequent Items to their Int Identifier.
   *                  The map should only contain frequent item.
   *
   * @return The internal repr of the inputted dataset. With properly placed zero delimiter.
   */
  private[fpm] def toDatabaseInternalRepr[Item: ClassTag](
      data: RDD[Array[Array[Item]]],
      itemToInt: Map[Item, Int]): RDD[Array[Int]] = {

    data.flatMap { itemsets =>
      val allItems = mutable.ArrayBuilder.make[Int]
      var containsFreqItems = false
      allItems += 0
      itemsets.foreach { itemsets =>
        val items = mutable.ArrayBuilder.make[Int]
        itemsets.foreach { item =>
          if (itemToInt.contains(item)) {
            items += itemToInt(item) + 1 // using 1-indexing in internal format
          }
        }
        val result = items.result()
        if (result.nonEmpty) {
          containsFreqItems = true
          allItems ++= result.sorted
          allItems += 0
        }
      }
      if (containsFreqItems) {
        Iterator.single(allItems.result())
      } else {
        Iterator.empty
      }
    }
  }

  private[fpm] def splitLarge(
     postfixes: RDD[Postfix],
     emptyPrefix: Prefix,
     minCount: Long,
     maxPatternLength: Int,
     maxLocalProjDBSize: Long): (mutable.ArrayBuffer[(Array[Int], Long)],
                                 mutable.Map[Int, Prefix]) = {

    // Local frequent patterns (prefixes) and their counts.
    val localFreqPatterns = mutable.ArrayBuffer.empty[(Array[Int], Long)]

    // Prefixes whose projected databases are large.
    var largePrefixes = mutable.Map(emptyPrefix.id -> emptyPrefix)
    // Prefixes whose projected databases are small.
    val smallPrefixes = mutable.Map.empty[Int, Prefix]

    while (largePrefixes.nonEmpty) {
      val numLocalFreqPatterns = localFreqPatterns.length
      logInfo(s"number of local frequent patterns: $numLocalFreqPatterns")
      if (numLocalFreqPatterns > 1000000) {
        logWarning(
          s"""
             | Collected $numLocalFreqPatterns local frequent patterns. You may want to consider:
             |   1. increase minSupport,
             |   2. decrease maxPatternLength,
             |   3. increase maxLocalProjDBSize.
           """.stripMargin)
      }
      logInfo(s"number of small prefixes: ${smallPrefixes.size}")
      logInfo(s"number of large prefixes: ${largePrefixes.size}")
      val largePrefixArray = largePrefixes.values.toArray
      val freqPrefixes = postfixes.flatMap { postfix =>
        largePrefixArray.flatMap { prefix =>
          postfix.project(prefix).genPrefixItems.map { case (item, postfixSize) =>
            ((prefix.id, item), (1L, postfixSize))
          }
        }
      }.reduceByKey { (cs0, cs1) =>
        (cs0._1 + cs1._1, cs0._2 + cs1._2)
      }.filter { case (_, cs) => cs._1 >= minCount }
        .collect()
      val newLargePrefixes = mutable.Map.empty[Int, Prefix]
      freqPrefixes.foreach { case ((id, item), (count, projDBSize)) =>
        val newPrefix = largePrefixes(id) :+ item
        localFreqPatterns += ((newPrefix.items :+ 0, count))
        if (newPrefix.length < maxPatternLength) {
          if (projDBSize > maxLocalProjDBSize) {
            newLargePrefixes += newPrefix.id -> newPrefix
          } else {
            smallPrefixes += newPrefix.id -> newPrefix
          }
        }
      }
      largePrefixes = newLargePrefixes
    }

    (localFreqPatterns, smallPrefixes)
  }


  private[fpm] def genProjPostfixes(
    sc: SparkContext,
    bcSmallPrefixes: Broadcast[mutable.Map[Int, Prefix]],
    postfixes: RDD[Postfix]): RDD[(Int, Iterable[Postfix])] = {
    val numSmallPrefixes = bcSmallPrefixes.value.size
    logInfo(s"number of small prefixes for local processing(anomaly): $numSmallPrefixes")

    var PrefixIdToProjPostfixes = sc.emptyRDD[(Int, Iterable[Postfix])]
    if (numSmallPrefixes > 0) {
      PrefixIdToProjPostfixes = postfixes.flatMap { postfix =>
        bcSmallPrefixes.value.values.map { prefix =>
          (prefix.id, postfix.project(prefix).compressed)
        }.filter(_._2.nonEmpty)
      }.groupByKey()
    }
    PrefixIdToProjPostfixes
  }

  /**
   * Find the complete set of frequent sequential patterns in the input sequences.
   * @param dataN ordered sequences of itemsets. We represent a sequence internally as Array[Int],
   *             where each itemset is represented by a contiguous sequence of distinct and ordered
   *             positive integers. We use 0 as the delimiter at itemset boundaries, including the
   *             first and the last position.
   * @param dataA ordered sequences of itemsets (anomaly).
   * @param totalCountN |D_normal|
   * @param totalCountA |D_anomaly|
   * @return an RDD of (frequent sequential pattern, count) pairs,
   * @see [[Postfix]]
   */
  private[fpm] def genFreqPatterns(
      dataN: RDD[Array[Int]],
      dataA: RDD[Array[Int]],
      totalCountN: Long,
      totalCountA: Long,
      minGR: Double,
      minCount: Long,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long): RDD[(Array[Int], Double, Long, Long)] = {
    val sc = dataN.sparkContext

    val DN = totalCountN.asInstanceOf[Double]
    val DA = totalCountA.asInstanceOf[Double]

    if (dataN.getStorageLevel == StorageLevel.NONE || dataA.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }

    val postfixesN = dataN.map(items => new Postfix(items))
    val (localFreqPatternsN, smallPrefixesN) = splitLarge(postfixesN,
                                                Prefix.emptyN,
                                                0,  // normal数据不做filter（否则所有小于minCount的countN都会变为0）
                                                maxPatternLength,
                                                maxLocalProjDBSize)
    val postfixesA = dataA.map(items => new Postfix(items))
    val (localFreqPatternsA, smallPrefixesA) = splitLarge(postfixesA,
                                                Prefix.emptyA,
                                                minCount,
                                                maxPatternLength,
                                                maxLocalProjDBSize)

    val normalFreqPatterns = localFreqPatternsN.map(cs => (cs._1.mkString(","), cs._2)).toMap
    var localCSP = sc.parallelize(localFreqPatternsA.map {
      case (freqPatternA, countA) =>
        val freqPatternAStr = freqPatternA.mkString(",")
        val countN = normalFreqPatterns.getOrElse(freqPatternAStr, 0L)
        (freqPatternA,
          (countA.asInstanceOf[Double] / DA) / (countN.asInstanceOf[Double] / DN),
          countA, countN)
    }.filter(_._2 >= minGR).toSeq, 1)

    val bcSmallPrefixesA = sc.broadcast(smallPrefixesA)
    val PrefixIdToProjPostfixesA = genProjPostfixes(sc, bcSmallPrefixesA, postfixesA)
    val bcSmallPrefixesN = sc.broadcast(smallPrefixesN)
    val PrefixIdToProjPostfixesN = genProjPostfixes(sc, bcSmallPrefixesN, postfixesN)

    val distributedCSP = PrefixIdToProjPostfixesA.leftOuterJoin(PrefixIdToProjPostfixesN).flatMap {
      case (id, (projPostfixesA, projPostfixesN)) =>
        val prefix = bcSmallPrefixesA.value(id)
        val localCSP = new LocalCSP(DN, DA, minGR, minCount, maxPatternLength - prefix.length)
        localCSP.run(projPostfixesA.toArray,
          projPostfixesN.getOrElse(Iterable.empty[Postfix]).toArray).map {
          case (csp, growthRate, countA, countN) =>
            (prefix.items ++ csp, growthRate, countA, countN)
        }
    }

    localCSP = localCSP ++ distributedCSP
    localCSP
  }

  /**
   * Represents a prefix.
   * @param items items in this prefix, using the internal format
   * @param length length of this prefix, not counting 0
   */
  private[fpm] class Prefix private (val items: Array[Int], val length: Int) extends Serializable {

    /** A unique id for this prefix. */
    var id: Int = findCachedId(items.mkString(","))
    if (id == -1) {
      id = Prefix.nextId
      cachePrefix(items.mkString(","), id)
    }

    /** Expands this prefix by the input item. */
    def :+(item: Int): Prefix = {
      require(item != 0)
      if (item < 0) {
        new Prefix(items :+ -item, length + 1)
      } else {
        new Prefix(items ++ Array(0, item), length + 1)
      }
    }
  }

  private[fpm] object Prefix {
    /** Internal counter to generate unique IDs. */
    private val counter: AtomicInteger = new AtomicInteger(-1)
    private val prefixCache: mutable.HashMap[String, Int] = mutable.HashMap.empty[String, Int]

    /** Gets the next unique ID. */
    private def nextId: Int = counter.incrementAndGet()

    private def findCachedId(prefixStr: String): Int = prefixCache.getOrElse(prefixStr, -1)

    private def cachePrefix(prefixStr: String, id: Int): this.type = {
      prefixCache += prefixStr -> id
      this
    }

    /** An empty [[Prefix]] instance. */
    val emptyA: Prefix = new Prefix(Array.empty, 0)
    val emptyN: Prefix = new Prefix(Array.empty, 0)
  }

  /**
   * An internal representation of a postfix from some projection.
   * We use one int array to store the items, which might also contains other items from the
   * original sequence.
   * Items are represented by positive integers, and items in each itemset must be distinct and
   * ordered.
   * we use 0 as the delimiter between itemsets.
   * For example, a sequence `(12)(31)1` is represented by `[0, 1, 2, 0, 1, 3, 0, 1, 0]`.
   * The postfix of this sequence w.r.t. to prefix `1` is `(_2)(13)1`.
   * We may reuse the original items array `[0, 1, 2, 0, 1, 3, 0, 1, 0]` to represent the postfix,
   * and mark the start index of the postfix, which is `2` in this example.
   * So the active items in this postfix are `[2, 0, 1, 3, 0, 1, 0]`.
   * We also remember the start indices of partial projections, the ones that split an itemset.
   * For example, another possible partial projection w.r.t. `1` is `(_3)1`.
   * We remember the start indices of partial projections, which is `[2, 5]` in this example.
   * This data structure makes it easier to do projections.
   *
   * @param items a sequence stored as `Array[Int]` containing this postfix
   * @param start the start index of this postfix in items
   * @param partialStarts start indices of possible partial projections, strictly increasing
   */
  private[fpm] class Postfix(
      val items: Array[Int],
      val start: Int = 0,
      val partialStarts: Array[Int] = Array.empty) extends Serializable {

    require(items.last == 0, s"The last item in a postfix must be zero, but got ${items.last}.")
    if (partialStarts.nonEmpty) {
      require(partialStarts.head >= start,
        "The first partial start cannot be smaller than the start index," +
          s"but got partialStarts.head = ${partialStarts.head} < start = $start.")
    }

    /**
     * Start index of the first full itemset contained in this postfix.
     */
    private[this] def fullStart: Int = {
      var i = start
      while (items(i) != 0) {
        i += 1
      }
      i
    }

    /**
     * Generates length-1 prefix items of this postfix with the corresponding postfix sizes.
     * There are two types of prefix items:
     *   a) The item can be assembled to the last itemset of the prefix. For example,
     *      the postfix of `<(12)(123)>1` w.r.t. `<1>` is `<(_2)(123)1>`. The prefix items of this
     *      postfix can be assembled to `<1>` is `_2` and `_3`, resulting new prefixes `<(12)>` and
     *      `<(13)>`. We flip the sign in the output to indicate that this is a partial prefix item.
     *   b) The item can be appended to the prefix. Taking the same example above, the prefix items
     *      can be appended to `<1>` is `1`, `2`, and `3`, resulting new prefixes `<11>`, `<12>`,
     *      and `<13>`.
     * @return an iterator of (prefix item, corresponding postfix size). If the item is negative, it
     *         indicates a partial prefix item, which should be assembled to the last itemset of the
     *         current prefix. Otherwise, the item should be appended to the current prefix.
     */
    def genPrefixItems: Iterator[(Int, Long)] = {
      val n1 = items.length - 1
      // For each unique item (subject to sign) in this sequence, we output exact one split.
      // TODO: use PrimitiveKeyOpenHashMap
      val prefixes = mutable.Map.empty[Int, Long]
      // a) items that can be assembled to the last itemset of the prefix
      partialStarts.foreach { start =>
        var i = start
        var x = -items(i)
        while (x != 0) {
          if (!prefixes.contains(x)) {
            prefixes(x) = n1 - i
          }
          i += 1
          x = -items(i)
        }
      }
      // b) items that can be appended to the prefix
      var i = fullStart
      while (i < n1) {
        val x = items(i)
        if (x != 0 && !prefixes.contains(x)) {
          prefixes(x) = n1 - i
        }
        i += 1
      }
      prefixes.toIterator
    }

    /** Tests whether this postfix is non-empty. */
    def nonEmpty: Boolean = items.length > start + 1

    /**
     * Projects this postfix with respect to the input prefix item.
     * @param prefix prefix item. If prefix is positive, we match items in any full itemset; if it
     *               is negative, we do partial projections.
     * @return the projected postfix
     */
    def project(prefix: Int): Postfix = {
      require(prefix != 0)
      val n1 = items.length - 1
      var matched = false
      var newStart = n1
      val newPartialStarts = mutable.ArrayBuilder.make[Int]
      if (prefix < 0) {
        // Search for partial projections.
        val target = -prefix
        partialStarts.foreach { start =>
          var i = start
          var x = items(i)
          while (x != target && x != 0) {
            i += 1
            x = items(i)
          }
          if (x == target) {
            i += 1
            if (!matched) {
              newStart = i
              matched = true
            }
            if (items(i) != 0) {
              newPartialStarts += i
            }
          }
        }
      } else {
        // Search for items in full itemsets.
        // Though the items are ordered in each itemsets, they should be small in practice.
        // So a sequential scan is sufficient here, compared to bisection search.
        val target = prefix
        var i = fullStart
        while (i < n1) {
          val x = items(i)
          if (x == target) {
            if (!matched) {
              newStart = i
              matched = true
            }
            if (items(i + 1) != 0) {
              newPartialStarts += i + 1
            }
          }
          i += 1
        }
      }
      new Postfix(items, newStart, newPartialStarts.result())
    }

    /**
     * Projects this postfix with respect to the input prefix.
     */
    private def project(prefix: Array[Int]): Postfix = {
      var partial = true
      var cur = this
      var i = 0
      val np = prefix.length
      while (i < np && cur.nonEmpty) {
        val x = prefix(i)
        if (x == 0) {
          partial = false
        } else {
          if (partial) {
            cur = cur.project(-x)
          } else {
            cur = cur.project(x)
            partial = true
          }
        }
        i += 1
      }
      cur
    }

    /**
     * Projects this postfix with respect to the input prefix.
     */
    def project(prefix: Prefix): Postfix = project(prefix.items)

    /**
     * Returns the same sequence with compressed storage if possible.
     */
    def compressed: Postfix = {
      if (start > 0) {
        new Postfix(items.slice(start, items.length), 0, partialStarts.map(_ - start))
      } else {
        this
      }
    }
  }

  /**
   * Represents a frequent sequence.
   * @param sequence a sequence of itemsets stored as an Array of Arrays
   * @param freq frequency
   * @tparam Item item type
   */
  @Since("1.5.0")
  class FreqSequence[Item] @Since("1.5.0") (
      @Since("1.5.0") val sequence: Array[Array[Item]],
      @Since("1.5.0") val growthRate: Double,
      @Since("1.5.0") val freqA: Long,
      @Since("1.5.0") val freqN: Long) extends Serializable {
    /**
     * Returns sequence as a Java List of lists for Java users.
     */
    @Since("1.5.0")
    def javaSequence: ju.List[ju.List[Item]] = sequence.map(_.toList.asJava).toList.asJava
  }
}

/**
 * Model fitted by [[CSP]]
 * @param freqSequences frequent sequences
 * @tparam Item item type
 */
@Since("1.5.0")
class CSPModel[Item] @Since("1.5.0") (
    @Since("1.5.0") val freqSequences: RDD[CSP.FreqSequence[Item]])
  extends Saveable with Serializable {

  /**
   * Save this model to the given path.
   * It only works for Item datatypes supported by DataFrames.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using `CSPModel.load`.
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("2.0.0")
  override def save(sc: SparkContext, path: String): Unit = {
    CSPModel.SaveLoadV1_0.save(this, path)
  }
}

@Since("2.0.0")
object CSPModel extends Loader[CSPModel[_]] {

  @Since("2.0.0")
  override def load(sc: SparkContext, path: String): CSPModel[_] = {
    CSPModel.SaveLoadV1_0.load(sc, path)
  }

  private[fpm] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private val thisClassName = "org.apache.spark.mllib.fpm.CSPModel"

    def save(model: CSPModel[_], path: String): Unit = {
      val sc = model.freqSequences.sparkContext
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Get the type of item class
      val sample = model.freqSequences.first().sequence(0)(0)
      val className = sample.getClass.getCanonicalName
      val classSymbol = runtimeMirror(getClass.getClassLoader).staticClass(className)
      val tpe = classSymbol.selfType

      val itemType = ScalaReflection.schemaFor(tpe).dataType
      val fields = Array(StructField("sequence", ArrayType(ArrayType(itemType))),
        StructField("freq", LongType))
      val schema = StructType(fields)
      val rowDataRDD = model.freqSequences.map { x =>
        Row(x.sequence, x.growthRate, x.freqA, x.freqN)
      }
      spark.createDataFrame(rowDataRDD, schema).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): CSPModel[_] = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val freqSequences = spark.read.parquet(Loader.dataPath(path))
      val sample = freqSequences.select("sequence").head().get(0)
      loadImpl(freqSequences, sample)
    }

    def loadImpl[Item: ClassTag](freqSequences: DataFrame, sample: Item): CSPModel[Item] = {
      val freqSequencesRDD = freqSequences.select("sequence", "freq").rdd.map { x =>
        val sequence = x.getSeq[scala.collection.Seq[Item]](0).map(_.toArray).toArray
        val growthRate = x.getDouble(1)
        val freqA = x.getLong(2)
        val freqN = x.getLong(3)
        new CSP.FreqSequence(sequence, growthRate, freqA, freqN)
      }
      new CSPModel(freqSequencesRDD)
    }
  }
}
