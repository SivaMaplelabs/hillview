/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;


import org.hillview.table.membership.SparseMembershipSet;

import java.util.Arrays;

/**
 * A set of integers.
 * A simplified version of IntOpenHash from fastutil http://fastutil.di.unimi.it
 */
@SuppressWarnings("NestedAssignment")
public class IntSet {
    private int[] key; /* The array of the linear probing */
    private int mask;
    private int n;  /* the size of the array - 1 */
    private boolean containsZero = false;  /* zero is reserved to signify an empty cell */
    private int size;

    private int maxFill;
    private final float f; /* the maximal load of the array */

    public IntSet(int expected, final float f) {
        if (expected < 10)
            expected = 100;
        if ((f > 0.0F) && (f <= 1.0F)) {
            this.f = f;
            this.n = HashUtil.arraySize(expected, f); /* size of array is power of two */
            this.mask = this.n - 1;
            this.maxFill = HashUtil.maxFill(this.n, f);
            this.key = new int[this.n + 1];
        } else {
            throw new IllegalArgumentException("Load factor must be greater than 0 and " +
                    "smaller than or equal to 1");
        }
    }

    /**
     * Create an IntSet
     * @param expected The expected number of elements.
     */
    public IntSet(int expected) {
        this(expected, 0.75F);
    }

    public IntSet() {
        this(16, 0.75F);
    }

    private int realSize() {
        return this.containsZero ? (this.size - 1) : this.size;
    }

    /**
     * @param k integer to add to the set
     * @return true if the set changed, false if the item is already in the set
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean add(final int k) {
        if (k == 0) {
            if (this.containsZero) {
                return false;
            }
            this.containsZero = true;
        } else {
            final int[] key = this.key;
            int pos;
            int curr;
            if ((curr = key[pos = HashUtil.murmurHash3(k) & this.mask]) != 0) {
                if (curr == k) {
                    return false;
                }
                while ((curr = key[(pos = (pos + 1) & this.mask)]) != 0) {
                    if (curr == k) {
                        return false;
                    }
                }
            }
            key[pos] = k;
        }
        if (this.size++ >= this.maxFill) {
            this.rehash(HashUtil.arraySize(this.size + 1, this.f));
        }
        return true;
    }

    /**
     * @param pos a location in the key[] array
     * @return the location of the next full slot after cursor. Operates on the iteratorKey array
     */
    public int getNext(int pos) {
        while ( this.key[pos & this.mask] == 0) { pos++; }
        return (pos & this.mask);
    }

    public int probe(int index) { return this.key[index & mask]; }

    public boolean contains(final int k) {
        if (k == 0) {
            return this.containsZero;
        } else {
            final int[] key = this.key;
            int curr;
            int pos;
            if((curr = key[pos = HashUtil.murmurHash3(k) & this.mask]) == 0) {
                return false;
            } else if(k == curr) {
                return true;
            } else {
                while((curr = key[(pos = (pos + 1) & this.mask)]) != 0) {
                    if(k == curr) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    private void rehash(final int newN) {
        HillviewLogger.instance.info("Rehashing", "from {0} to {1}", this.realSize(), newN);
        final int[] key = this.key;
        final int mask = newN - 1;
        final int[] newKey = new int[newN + 1];
        int i = this.n;
        int pos;
        for(int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
            do {
                --i;
            } while(key[i] == 0);

            if (newKey[pos = HashUtil.murmurHash3(key[i]) & mask] != 0) {
                //noinspection StatementWithEmptyBody
                while (newKey[(pos = (pos + 1) & mask)] != 0) {}
            }
        }
        this.n = newN;
        this.mask = mask;
        this.maxFill = HashUtil.maxFill(this.n, this.f);
        this.key = newKey;
    }

    /**
     * @return a deep copy of IntSet
     */
    public IntSet copy() {
        final IntSet newSet = new IntSet(1, this.f);
        newSet.n = this.n;
        newSet.mask = this.mask;
        newSet.maxFill = this.maxFill;
        newSet.size = this.size;
        newSet.containsZero = this.containsZero;
        newSet.key = new int[this.n + 1];
        System.arraycopy(this.key, 0, newSet.key, 0, this.key.length);
        return newSet;
    }

    public int arraySize() { return this.key.length; }

    /**
     * Uses the class Randomness to sample k items without replacement. If k > size returns the
     * the entire set.
     * @param k the number of items to be sampled
     * @return an IntSet of the sample
     */
    public IntSet sample(final int k, final long seed) {
        if (k >= this.size)
            return this;
        final IntSet sampleSet = new IntSet(k);
        final Randomness psg = new Randomness(seed);
        int sampleSize;
        int randomKey = psg.nextInt(this.n);
        if ((this.containsZero) && (randomKey == 0)) {  // sampling zero is done separately
            sampleSet.add(0);
            sampleSize = k-1;
        }
        else sampleSize = k;
        randomKey = psg.nextInt();
        for (int samples = 0; samples < sampleSize; samples++) {
            while (this.key[randomKey & this.mask] == 0)
                randomKey++;
            sampleSet.add(this.key[randomKey & this.mask]);
            randomKey++;
        }
        return sampleSet;
    }

    public IntSetIterator getIterator() {
        return new IntSetIterator(this);
    }

    /* Iterator for IntSet. Returns -1 when done. Assumes IntSet is not mutated */
    public static class IntSetIterator {
        private int pos;
        private int c;
        private boolean mustReturnZero;
        private final int[] iteratorKey;

        private IntSetIterator(IntSet set) {
            this.pos = set.n;
            this.c = set.size;
            this.mustReturnZero = set.containsZero;
            if (this.c < SparseMembershipSet.thresholdSortedIterator) {
                this.iteratorKey = set.key;
            } else {
                this.iteratorKey = Arrays.copyOf(set.key, set.key.length);
                Arrays.sort(this.iteratorKey);
            }
        }

        boolean hasNext() {
            return this.c != 0;
        }

        public int getNext() {
            if (!this.hasNext())
                return -1;
            --this.c;
            if (this.mustReturnZero) {
                this.mustReturnZero = false;
                return 0;
            }
            while (this.pos >= 0) {
                if (this.iteratorKey[this.pos] != 0) {
                    this.pos--;
                    return this.iteratorKey[this.pos + 1];
                }
                this.pos--;
            }
            return -1;
        }
    }
}

