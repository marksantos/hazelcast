/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.examples;

public enum MemoryUnit {

	BYTES {
		public long convert(long value, MemoryUnit m) {return m.toBytes(value);}
		public long toBytes(long value) {return value;}
		public long toKiloBytes(long value) {return divideByAndRoundToInt(value, K);}
		public long toMegaBytes(long value) {return divideByAndRoundToInt(value, M);}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, G);}
	},
	KILOBYTES {
		public long convert(long value, MemoryUnit m) {return m.toKiloBytes(value);}
		public long toBytes(long value) {return value * K;}
		public long toKiloBytes(long value) {return value;}
		public long toMegaBytes(long value) {return divideByAndRoundToInt(value, K);}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, M);}
	},
	MEGABYTES {
		public long convert(long value, MemoryUnit m) {return m.toMegaBytes(value);}
		public long toBytes(long value) {return value * M;}
		public long toKiloBytes(long value) {return value * K;}
		public long toMegaBytes(long value) {return value;}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, K);}
	},
	GIGABYTES {
		public long convert(long value, MemoryUnit m) {return m.toGigaBytes(value);}
		public long toBytes(long value) {return value * G;}
		public long toKiloBytes(long value) {return value * M;}
		public long toMegaBytes(long value) {return value * K;}
		public long toGigaBytes(long value) {return value;}
	};

	public static final int K = 1 << 10;
    public static final int M = 1 << 20;
    public static final int G = 1 << 30;

	public abstract long convert(long value, MemoryUnit m);
	public abstract long toBytes(long value);
	public abstract long toKiloBytes(long value);
	public abstract long toMegaBytes(long value);
	public abstract long toGigaBytes(long value);

    public static int divideByAndRoundToInt(double d, int k) {
        return (int) Math.rint(d / k);
    }
}
