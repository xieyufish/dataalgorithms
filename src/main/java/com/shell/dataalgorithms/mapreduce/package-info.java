/**
 * 实现mapreduce中secondary sort这个特性.
 * secondary sort: 当我们在reduce端想实现自己的group特性,而不是跟map端一样的group分组特性,那么我们就可以通过
 * job.setGroupingComparatorClass()这个方法来设置reduce端的group特性,这个方法在reduce拉取map的输出结果
 * 时会对中间结果重新group,经过这个类group到一个组中的记录会被传到同一个reduce任务中去执行reduce操作
 */
/**
 * @author Administrator
 *
 */
package com.shell.dataalgorithms.mapreduce;