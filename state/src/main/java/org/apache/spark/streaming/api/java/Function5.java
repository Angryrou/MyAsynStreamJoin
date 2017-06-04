package org.apache.spark.streaming.api.java;

import java.io.Serializable;

/**
 * Created by kawhi on 03/06/2017.
 */
public interface Function5<T1, T2, T3, T4, T5, R> extends Serializable {
    R call(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5) throws Exception;
}

