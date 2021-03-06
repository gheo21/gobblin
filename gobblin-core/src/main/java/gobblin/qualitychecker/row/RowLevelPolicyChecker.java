/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.util.FinalState;


public class RowLevelPolicyChecker implements Closeable, FinalState {

  private final List<RowLevelPolicy> list;
  private boolean errFileOpen;
  private final Closer closer;
  private RowLevelErrFileWriter writer;

  public RowLevelPolicyChecker(List<RowLevelPolicy> list) {
    this.list = list;
    this.errFileOpen = false;
    this.closer = Closer.create();
    this.writer = this.closer.register(new RowLevelErrFileWriter());
  }

  public boolean executePolicies(Object record, RowLevelPolicyCheckResults results) throws IOException {
    for (RowLevelPolicy p : this.list) {
      RowLevelPolicy.Result result = p.executePolicy(record);
      results.put(p, result);

      if (result.equals(RowLevelPolicy.Result.FAILED)) {
        if (p.getType().equals(RowLevelPolicy.Type.FAIL)) {
          throw new RuntimeException("RowLevelPolicy " + p + " failed on record " + record);
        } else if (p.getType().equals(RowLevelPolicy.Type.ERR_FILE)) {
          if (!errFileOpen) {
            this.writer.open(new Path(p.getErrFileLocation(), p.toString().replaceAll("\\.", "-") + ".err"));
            this.writer.write(record);
          } else {
            this.writer.write(record);
          }
          errFileOpen = true;
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (errFileOpen) {
      this.closer.close();
    }
  }

  /**
   * Get final state for this object, obtained by merging the final states of the
   * {@link gobblin.qualitychecker.row.RowLevelPolicy}s used by this object.
   * @return Merged {@link gobblin.configuration.State} of final states for
   *                {@link gobblin.qualitychecker.row.RowLevelPolicy} used by this checker.
   */
  public State getFinalState() {
    State state = new State();
    for (RowLevelPolicy policy : this.list) {
      state.addAll(policy.getFinalState());
    }
    return state;
  }
}
