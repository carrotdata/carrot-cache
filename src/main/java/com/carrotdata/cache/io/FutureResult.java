/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.cache.io;


public abstract class FutureResult<T> {

  public static interface CompletionHandler {
    
    public void complete();
    
    public void reset();
    
  }
  
  protected T buffer;

  protected int offset;

  protected volatile int result;

  protected volatile boolean done = false;

  protected Throwable t;

  protected boolean submitted = false;

  protected CompletionHandler handler;
  
  protected Object attachment;
  
  
  public FutureResult(T buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
  }

  public abstract int available();
  
  public void reset() {

    this.done = false;
    this.result = 0;
    this.t = null;
    this.submitted = false;
    this.attachment = null;
 
  }

  public Object getAttachment() {
    return this.attachment;
  }
  
  public void setAttachment(Object o) {
    this.attachment = o;
  }
  
  public void setCompletionHandler(CompletionHandler handler) {
    this.handler = handler;
  }
  
  public CompletionHandler getCompletionHandler() {
    return this.handler;
  }
  
  public synchronized void setDone(boolean b) {
    this.done = b;
    if (this.handler != null) {
      handler.complete();
    }
  }

  public synchronized boolean isDone() {
    return this.done;
  }

  public void submit() {
    this.submitted = true;
  }

  public boolean isSubmitted() {
    return submitted;
  }

  public T getBuffer() {
    return this.buffer;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getResult() {
    return this.result;
  }

  public void setResult(int total) {
    this.result = total;
  }

  public boolean isFailed() {
    return this.t != null;
  }

  public Throwable getError() {
    return this.t;
  }

  public void setError(Throwable t) {
    this.t = t;
  }
}