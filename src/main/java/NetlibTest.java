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
import com.github.fommil.netlib.BLAS;
import java.util.Random;

/**
 * Code to do some simple speed comparisons between netlib java JNI and Java implementations.
 */
public class NetlibTest {
    private static final int MAX_SIZE = 10;

    public static void main(String[] args) {
        //multiply various sized matrices
        for (int i = 1; i < MAX_SIZE; i++) {
            int sz = i * 1000;
            // initialize arrays
            double[] mat1 = new double[sz * sz];
            double[] mat2 = new double[sz * sz];
            double[] matOut = new double[sz * sz];
            Random rand = new Random();
            for (int j = 0; j < sz * sz; j++) {
                mat1[j] = 0;//rand.nextDouble();
                mat2[j] = 1;//rand.nextDouble();
                matOut[j] = 0;
            }
            long time1 = System.nanoTime();//bean.getCurrentThreadUserTime();
            BLAS.getInstance().dgemm("N", "N",
                    sz, sz, sz,
                    1.0, mat1, sz, mat2, sz,
                    0.0, matOut, sz);
            double elapsed = (System.nanoTime() - time1) / 1000000000d;
            System.out.printf("%.4f\n", elapsed, sz, sz);
        }
    }
}