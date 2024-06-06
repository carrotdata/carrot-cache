/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.util;

/**
 * Utility class that handles byte arrays, conversions to/from other types, comparisons, hash code
 * generation, manufacturing keys for HashMaps or HashSets, and can be used as key in maps or trees.
 */
public class Bytes {

  /** Size of boolean in bytes */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /** Size of byte in bytes */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /** Size of char in bytes */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /** Size of double in bytes */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /** Size of float in bytes */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /** Size of int in bytes */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /** Size of long in bytes */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /** Size of short in bytes */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

}
