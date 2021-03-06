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

import org.apache.commons.lang3.builder.HashCodeBuilder

/**
 * A rectangle with a left corner of (x, y) and a right upper corner of (x2, y2)
 */
case class DBSCANRectangle(min_corners: Array[Double], max_corners: Array[Double]) {

  /**
   * Returns whether other is contained by this box
   */
  def contains(other: DBSCANRectangle): Boolean = {
    val check_min = (min_corners zip other.min_corners).map{case (lim,other) => lim <= other}.reduce(_ && _)
    val check_max = (max_corners zip other.max_corners).map{case (lim,other) => other <= lim}.reduce(_ && _)
    check_min && check_max
  }

  /**
   * Returns whether point is contained by this box
   */
  def contains(point: DBSCANPoint): Boolean = {
    val check_min = (min_corners zip point.featuresA).map{case (lim,other) => lim <= other}.reduce(_ && _)
    val check_max = (max_corners zip point.featuresA).map{case (lim,other) => other <= lim}.reduce(_ && _)
    check_min && check_max
  }

  /**
   * Returns a new box from shrinking this box by the given amount
   */
  def shrink(amount: Double): DBSCANRectangle = {
    val new_min_corners = min_corners.map(_ + amount)
    val new_max_corners = max_corners.map(_ - amount)
    DBSCANRectangle(new_min_corners, new_max_corners)
  }

  /**
   * Returns a whether the rectangle contains the point, and the point
   * is not in the rectangle's border
   */
  def almostContains(point: DBSCANPoint): Boolean = {
    val check_min = (min_corners zip point.featuresA).map{case (lim,other) => lim < other}.reduce(_ && _)
    val check_max = (max_corners zip point.featuresA).map{case (lim,other) => other < lim}.reduce(_ && _)
    check_min && check_max
  }

  override def toString : String = {
    return "DBSCANRectangle(" +
    "min_corners: " + min_corners.mkString(",") +
    " ; max_corners: " + max_corners.mkString(",") + ")"
  }

  override def hashCode:Int = {
     // you pick a hard-coded, randomly chosen, non-zero, odd number
     // ideally different for each class
     return new HashCodeBuilder(17, 37).
       append(min_corners).
       append(max_corners).
       toHashCode()
   }
  
  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[DBSCANRectangle]) {
      this.hashCode.equals(that.hashCode) 
    }
    else { false }
    /*that match {
      case that: DBSCANRectangle => { this.hashCode.equals(that.hashCode) }
      case _ => { false }
   }*/
  }
}
