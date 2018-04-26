/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

/**
 * An interface to estimate the size of payload in memory
 * @param <T>
 */
public interface SizeEstimator<T> {

  /**
   * This method is used to estimate the size of a payload in memory.
   * The default implementation returns the total allocated size, in bytes, of the object
   * and all other objects reachable from it
   */
  long sizeEstimate(T t);
}