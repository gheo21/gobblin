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

package gobblin.util;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;


/**
 * A class for obtaining record counts from data.
 */
public abstract class RecordCountProvider {

  /**
   * Get record count from a given {@link Path}.
   */
  public abstract long getRecordCount(Path path);

  /**
   * Convert a {@link Path} from another {@link RecordCountProvider} so that it can be used
   * in {@link #getRecordCount(Path)} of this {@link RecordCountProvider}.
   */
  public Path convertPath(Path path, RecordCountProvider src) {
    if (this.getClass().equals(src.getClass())) {
      return path;
    } else {
      throw getNotImplementedException(src);
    }
  }

  protected NotImplementedException getNotImplementedException(RecordCountProvider src) {
    return new NotImplementedException(String.format("converting from %s to %s is not implemented",
        src.getClass().getName(), this.getClass().getName()));
  }
}
