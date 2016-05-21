#Cluster analysis of genetic variants and metadata with Spark

##Overview

We use three different clustering algorithms on Apache Spark, adapting the K-Means, CAST and DBSCAN approaches to distributed computing in Scala.

These algorithms have been applied to the following datasets:
* Acute Inflammations UCI dataset (non-genetic dataset to test the methods)
* 1000 Genomes (healthy patients from different ethnic populations)
* DDD cohort (patients with different developmental diseases)
* 1000G against DDD (one dataset against the other)

As Scala Spark code can be very dense with its pipelined operations, complete documentation is often needed to grasp its content. The Spark Notebook format (.snb) was chosen for its handling of elegant markups amongst the code, which facilitates the insertion and typesetting of such documentation, as well as for the possibility it offers to execute bits of code independently.

This repository thus contains the notebooks corresponding to the application of each algorithm to these datasets, using different representations.

*Note that the DBSCAN approach was forked from [https://github.com/irvingc/dbscan-on-spark], which was made available by Irving Cordova under the Apache 2.0 license. Originally limited to two-dimensional input data, the algorithm was refactored, extended to the handling of multiple dimensions, and of labeled data. Due to the original source of this approach, the code is available as a Scala package rather than a Notebook.*

##Notebooks

###Acute Inflammations

A first introduction to the code is made through the [Acute Inflammations dataset](https://archive.ics.uci.edu/ml/datasets/Acute+Inflammations), from the UCI Machine Learning repository.
The dataset is small enough for the algorithms to run efficiently on a single machine. The corresponding files to execute are the following:
* CAST (Acute Inflammations)
* K-Means (Acute Inflammations)
* The DBSCAN package, with *DBSCAN_acute_infl.scala* as main file

###Genetic variants dataset

For the other datasets, the cluster analysis is divided into two steps, namely the preprocessing step and the clustering itself. Preprocessing consists in preparing the inputs necessary for the subsequent operations: K-Means and DBSCAN require an alignment of features as input, and CAST requires a similarity matrix S and a set of elements U.

These notebooks can be run locally for a very low number of patients (< 10), which allows easy testing. For a larger number of samples, they can quickly be converted into Scala files (File > Download as > Scala in Spark Notebook) and made into standalone applications to submit to a computer cluster.

###1000 Genomes
The main possible combinations of applications to work on 1000 Genomes data:
* Preprocessing - Alignment of variants (1000G) + K-Means (1000G)
* Preprocessing - Alignment of metadata (1000G) + K-Means (1000G)
* Preprocessing - Similarity matrix on variants (1000G) + CAST (1000G)
* Preprocessing - Similarity matrix on metadata (1000G) + CAST (1000G)
* Preprocessing - Alignment of metadata (1000G) + DBSCAN package

###DDD cohort
The main possible combinations of applications to work on DDD data:
* Preprocessing - Alignment of variants (DDD) + K-Means (DDD)
* Preprocessing - Alignment of metadata (DDD) + K-Means (DDD)
* Preprocessing - Similarity matrix on variants (DDD) + CAST (DDD)
* Preprocessing - Similarity matrix on metadata (DDD) + CAST (DDD)
* Preprocessing - Alignment of metadata (DDD) + DBSCAN package with *DBSCAN_DDD_metadata.scala* as main file
* Preprocessing - Similarity matrix on metadata, with impact analysis (DDD) + K-Means, with impact analysis (DDD)
* Preprocessing - Similarity matrix on metadata, with impact analysis (DDD) + CAST, with impact analysis (DDD)

###Comparing 1000 Genomes and DDD data
The main possible combinations of applications to work on these two datasets together:
* Preprocessing - Alignment of variants (1000G against DDD) + K-Means (1000G against DDD)
* Preprocessing - Alignment of metadata (1000G against DDD) + K-Means (1000G against DDD)
