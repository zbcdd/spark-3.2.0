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

package org.apache.spark.mllib.fpm;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.CSP.FreqSequence;
import org.apache.spark.util.Utils;

public class JavaCSPSuite extends SharedSparkSession {

  @Test
  public void runCSP() {
    JavaRDD<List<List<Integer>>> sequencesN = jsc.parallelize(Arrays.asList(
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 6)),
      Arrays.asList(Arrays.asList(1), Arrays.asList(2, 3), Arrays.asList(1, 2), Arrays.asList(6)),
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5, 6)),
      Arrays.asList(Arrays.asList(6))
    ), 2);
    JavaRDD<List<List<Integer>>> sequencesA = jsc.parallelize(Arrays.asList(
            Arrays.asList(Arrays.asList(1, 3), Arrays.asList(1, 3, 6)),
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 3, 6), Arrays.asList(1, 2)),
            Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(2, 5, 6)),
            Arrays.asList(Arrays.asList(1, 2, 6))
    ), 2);
    CSP csp = new CSP()
      .setMinGR(1.3)
      .setMinSupport(0.3)
      .setMaxPatternLength(5);
    CSPModel<Integer> model = csp.run(sequencesN, sequencesA);
    JavaRDD<FreqSequence<Integer>> freqSeqs = model.freqSequences().toJavaRDD();
    List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
    Assert.assertEquals(20, localFreqSeqs.size());
    // Check that each frequent sequence could be materialized.
    for (CSP.FreqSequence<Integer> freqSeq : localFreqSeqs) {
      List<List<Integer>> seq = freqSeq.javaSequence();
      double gR = freqSeq.growthRate();
      long countA = freqSeq.freqA();
      long countN = freqSeq.freqN();
//      System.out.println("Seq: " + seq.toString() + " Growth Rate: " + gR + " CountA: " + countA + " CountN: " + countN);
    }
  }

//  @Test
//  public void runCSPSaveLoad() {
//    JavaRDD<List<List<Integer>>> sequences = jsc.parallelize(Arrays.asList(
//      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
//      Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
//      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
//      Arrays.asList(Arrays.asList(6))
//    ), 2);
//    CSP csp = new CSP()
//      .setMinSupport(0.5)
//      .setMaxPatternLength(5);
//    CSPModel<Integer> model = csp.run(sequences);
//
//    File tempDir = Utils.createTempDir(
//      System.getProperty("java.io.tmpdir"), "JavaCSPSuite");
//    String outputPath = tempDir.getPath();
//
//    try {
//      model.save(spark.sparkContext(), outputPath);
//      @SuppressWarnings("unchecked")
//      CSPModel<Integer> newModel =
//          (CSPModel<Integer>) CSPModel.load(spark.sparkContext(), outputPath);
//      JavaRDD<FreqSequence<Integer>> freqSeqs = newModel.freqSequences().toJavaRDD();
//      List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
//      Assert.assertEquals(5, localFreqSeqs.size());
//      // Check that each frequent sequence could be materialized.
//      for (CSP.FreqSequence<Integer> freqSeq : localFreqSeqs) {
//        List<List<Integer>> seq = freqSeq.javaSequence();
//        long freq = freqSeq.freq();
//      }
//    } finally {
//      Utils.deleteRecursively(tempDir);
//    }
//
//
//  }
}
