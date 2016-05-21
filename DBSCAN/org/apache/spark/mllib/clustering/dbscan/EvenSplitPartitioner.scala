/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

import org.apache.spark.Logging

/**
 * Helper methods for calling the partitioner
 */
object EvenSplitPartitioner {

  def partition(
    toSplit: RDD[(DBSCANRectangle, Int)],
    maxPointsPerPartition: Long,
    minimumRectangleSize: Double): List[(DBSCANRectangle, Int)] = {
    new EvenSplitPartitioner(maxPointsPerPartition, minimumRectangleSize)
      .findPartitions(toSplit)
  }

}

class EvenSplitPartitioner(
  maxPointsPerPartition: Long,
  minimumRectangleSize: Double) extends Logging {

  type RectangleWithCount = (DBSCANRectangle, Int)

  def findPartitions(toSplit: RDD[RectangleWithCount]): List[RectangleWithCount] = {

    val boundingRectangle = findBoundingRectangle(toSplit)

    def pointsIn = pointsInRectangle(toSplit, _: DBSCANRectangle)

    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

	toPartition.foreach{
	case (rectangle, count) =>
	logWarning(s"BOUNDING: Min bounding rectangle of $count points : $rectangle")
	}

    logTrace("About to start partitioning")
    val partitions = partition(toPartition, partitioned, pointsIn)
    logTrace("Done")

    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })
  }

  @tailrec
  private def partition(
    remaining: List[RectangleWithCount],
    partitioned: List[RectangleWithCount],
    pointsIn: (DBSCANRectangle) => Int): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {

          if (canBeSplit(rectangle)) {
            //logWarning(s"About to split: $rectangle")
            def cost = (r: DBSCANRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
            val (split1, split2) = split(rectangle, cost)
            //logWarning(s"Found split: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)
          } else {
            //logWarning(s"Can't split: $rectangle can't fit two rectangles of size $minimumRectangleSize")
            partition(rest, (rectangle, count) :: partitioned, pointsIn)
          }

        } else {
	  //logWarning(s"No need to split: $rectangle has $count points, < partition size ($maxPointsPerPartition points)")
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }

      case Nil => partitioned

    }

  }

  def split(
    rectangle: DBSCANRectangle,
    cost: (DBSCANRectangle) => Int): (DBSCANRectangle, DBSCANRectangle) = {

    val smallestSplit =
      findPossibleSplits(rectangle)
        .reduceLeft {
          (smallest, current) =>

            if (cost(current) < cost(smallest)) {
              current
            } else {
              smallest
            }

        }

    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
   * Returns the box that covers the space inside boundary that is not covered by box
   */
private def complement(box: DBSCANRectangle, boundary: DBSCANRectangle): DBSCANRectangle =
  if ((box.min_corners zip boundary.min_corners).map{case (box,b) => box == b}.reduce(_ && _)) { //mins are equal
      if ((box.max_corners zip boundary.max_corners).map{case (box,b) => b >= box}.reduce(_ && _)) { //boundary has bigger max than box
        
        val nb_differences = (box.max_corners zip boundary.max_corners).
        map{ case (box,b) => if (box == b) { 0 } else { 1 } }.reduce(_ + _)
        
        if (nb_differences <= 1) {
          
          val new_min_corners = (box.min_corners zip (box.max_corners zip boundary.max_corners)).
          map{
            case (box_min, (box_max, b_max)) =>
            if (box_max == b_max) { box_min }
            else { box_max }
          }
          
          DBSCANRectangle(new_min_corners, boundary.max_corners)
        //must be different in only one dimension for BOTH normal and complementary to be proper rectangles
        //mmmh this may fail because when we did min(x,y) i did it independently for every dimension so may not match this
          
        } else {
          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("rectangle is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }
  //complement(DBSCANRectangle(Array(0, 0, 0), Array(2, 4, 4)), DBSCANRectangle(Array(0, 0, 0), Array(4, 4, 4))).min_corners.mkString(",")
  //=> gives (2,0,0) so ok
  

  /*  
   * Returns all the possible ways in which the given box can be split
   */
  private def findPossibleSplits(box: DBSCANRectangle): Set[DBSCANRectangle] = {

    val range_splits = (box.min_corners.indices zip (box.min_corners zip box.max_corners)).flatMap{
      case (i, (min_c, max_c)) =>
      val possibilities = (min_c + minimumRectangleSize) until max_c by minimumRectangleSize
      possibilities.map(x => (i, x))
    }

    val all_splits = range_splits.map{
      case (i, new_val) =>
      var new_max = box.max_corners.clone() //need a deepcopy of max_corners or it gets changed along the way!
      new_max(i) = new_val
      DBSCANRectangle(box.min_corners, new_max)
    }

    //logTrace(s"Possible splits : $splits")

    all_splits.toSet
  }
//findPossibleSplits(DBSCANRectangle(Array(0, 0), Array(10, 10))).foreach{x => println(x.max_corners.mkString(","))}
// 4.0,10.0  ;  8.0,10.0  ;  10.0,4.0  ; 10.0,8.0 => ok

  
  /**
   * Returns true if the given rectangle can be split into at least two rectangles of minimum size
   */
  private def canBeSplit(box: DBSCANRectangle): Boolean = {
    (box.min_corners zip box.max_corners).map{
    case (min_c, max_c) => max_c - min_c >= minimumRectangleSize * 2
    }.reduce(_ || _)
  }

  def pointsInRectangle(space: RDD[RectangleWithCount], rectangle: DBSCANRectangle): Int = {
    space
      .filter({ case (current, _) => rectangle.contains(current) })
      .fold((DBSCANRectangle(Array(), Array()), 0)) {
        case ((r, total), (_, count)) => (r, total + count)
        //the DBSCANRectangle(Array(), Array()) and r are just here so types are correct...
      }._2
  }

  def findBoundingRectangle(rectanglesWithCount: RDD[RectangleWithCount]): DBSCANRectangle = {
    val nb_features = rectanglesWithCount.first._1.min_corners.size
    val max_values = Array.fill(nb_features){Double.MaxValue}
    val min_values = Array.fill(nb_features){Double.MinValue}

    //most extreme boundaries (for min corner, it's max values!)
    val invertedRectangle = DBSCANRectangle(max_values, min_values)

    //we fuse the rectangles into each other to get the largest one containing all others
    rectanglesWithCount.keys.fold(invertedRectangle) {
      case (bounding, c) => //bounding=current best, c=challenger
      val mins = (bounding.min_corners zip c.min_corners).map{case (x,y) => x.min(y)}
      val maxs = (bounding.max_corners zip c.max_corners).map{case (x,y) => x.max(y)}
      DBSCANRectangle(mins, maxs)
    }

  }

}
