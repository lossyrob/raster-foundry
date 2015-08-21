package com.azavea.foundry.etc

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.spark.tiling._
import geotrellis.proj4._
import java.io._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.commons.io.FileUtils
import java.nio.file.Files


import sys.process._
import spire.syntax.cfor._

// Tiling algorithm I'm working out for OAM
object TilingAlgorithm {
  def processLogger(s: String) =
    ProcessLogger({ x: String => print(s) ; scala.Console.out.flush() }, { x: String => scala.Console.err.println(x) })

  def zoomFor(resolution: Double): Int = {
    val WEBMECATOR_WIDTH = 20026376.39 * 2

    def _zoomFor(z: Int): Int = {
      val r2 = WEBMECATOR_WIDTH / (math.pow(2, z + 1) * 256)
      val r1 = WEBMECATOR_WIDTH / (math.pow(2, z) * 256)
      if(r2 < resolution) {
        val dRes = r1 - resolution
        val dZoom = r1 - r2
        if(dRes * 3 < dZoom) {
          z
        } else {
          z + 1
        }
      } else {
        _zoomFor(z + 1)
      }
    }

    return _zoomFor(2)
  }

  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[8]")
        .setAppName("Raster Foundry ETL")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.hadoop.KryoRegistrator")

    new SparkContext(conf)
  }

  val outputDir = "/Users/rob/proj/oam/data/overzoom"

  val layoutScheme = ZoomedLayoutScheme(256)
  val worldExtent = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244)  

  // local
  def getGridCells(path: String): (Int, Set[(Int, Int)]) = {
    println(path)
    val tags = TiffTagsReader.read(path)
    val extent = tags.extent
    val (c, r) = (tags.cols, tags.rows)
    // val z = {
    //   val z = zoomFor(extent.width / c)
    //   if(z == 18) 15 else z
    // }
    val z = zoomFor(extent.width / c)
    val LayoutLevel(_, TileLayout(cols, rows, _, _)) = layoutScheme.levelFor(z)
    val mapTransform = MapKeyTransform(worldExtent, cols, rows)
    (z, mapTransform(extent).coords.toSet)
  }

  // local
  def getImagePaths(workingDir: String, z: Int, cells: Set[(Int, Int)]): Seq[String] =
    cells.toSeq.map { case (x, y) =>
      s"$workingDir/$z/$x/$y.tif"
    }

  // local
  def buildVrt(z: Int, images: Seq[String], initial: Boolean = false): String = {
    val outPath = 
      if(initial) {
        ensureDirs(s"$outputDir/vrt/initial-$z.vrt")
      } else {
        ensureDirs(s"$outputDir/vrt/$z.vrt")
      }

    val args = images.mkString(" ")
    val cmd =
      s"gdalbuildvrt -srcnodata 0 -overwrite $outPath $args"
    println(cmd)
    val result =  cmd !

    if(result != 0) sys.error(s"Failed: $cmd")
    outPath
  }

  def reproject(path: String, dir: String): String = {
    val outPath = ensureDirs((new File(dir, new File(path).getName())).getAbsolutePath)

    val options = Seq(
      "-q", "-overwrite",
//      "-wo", "NUM_THREADS=ALL_CPUS",
      "-multi",
      "-co", "tiled=yes",
      "-co", "compress=lzw",
      "-co", "predictor=2",
      "-r", "bilinear").mkString(" ")


    val reprojectCommand =
      s"gdalwarp -t_srs EPSG:3857 $options $path $outPath"
    println(reprojectCommand)
    val result =  reprojectCommand !

    if(result != 0) sys.error(s"Failed: $reprojectCommand")
    outPath
  }

  def resample(inPath: String, outPath: String, window: Extent): Unit = {
    val options = Seq(
      "-q",
      f"-te ${window.xmin}%1.6f ${window.ymin}%1.6f ${window.xmax}%1.6f ${window.ymax}%1.6f",
      "-ts", "256", "256",
      "-wm", 256, // allow GDAL to work with larger chunks (diminishing returns after 500MB, supposedly)
      "-wo", "NUM_THREADS=ALL_CPUS",
      "-multi", "-overwrite",
      "-r", "bilinear"
).mkString(" ")


    val cmd =
      s"gdalwarp -t_srs EPSG:3857 $options $inPath ${outPath}"
//    println(cmd)
    val result =  cmd !(processLogger("."))

    if(result != 0) sys.error(s"Failed: $cmd")
  }

  def toPng(inPath: String, outPath: String): Unit = {
    val cmd =
      s"gdal_translate -a_nodata 0 -of PNG $inPath $outPath"
//    println(cmd)
    val result =  cmd !(processLogger("*"))

    if(result != 0) sys.error(s"Failed: $cmd")
  }

  def ensureDirs(path: String): String = {
    new File(path).getParentFile.mkdirs()
    path
  }

  // This is the activity.
  def tile(vrt: String, z: Int, cells: Set[(Int, Int)], workingDir: String)(implicit sc: SparkContext): Unit = {
    println(s"TILING $z with ${cells.size} cells.")
    val LayoutLevel(_, TileLayout(cols, rows, _, _)) = layoutScheme.levelFor(z)
    val mapTransform = MapKeyTransform(worldExtent, cols, rows)
    sc.parallelize( cells.toSeq ).foreach { case (x, y) =>
      val ext = mapTransform(x, y)
      val outPath = ensureDirs(s"$workingDir/$z/$x/$y.tif")
      val pngPath = ensureDirs(s"$outputDir/$z/$x/$y.png")
      // println(s"RESAMPLE $vrt $outPath, $ext")
      // println(s"TO PNG $outPath $pngPath")
     resample(vrt, outPath, ext)
     toPng(outPath, pngPath)
    }
    println(s"\nDONE RESAMPLING ZOOM $z")
  }

  def tile(initialVrt: String, z1: Int, z2: Int, cells: Set[(Int, Int)], workingDir: String)(implicit sc: SparkContext): (String, Set[(Int, Int)]) = {
    println(s"TILING BETWEEN $z1 and $z2 with ${cells.size} initial cells.")
    (z1 until z2 by -1).foldLeft((initialVrt, cells)) { (theseValues, z) =>
      val (thisVrt, theseCells) = theseValues
      tile(thisVrt, z, theseCells, workingDir)
      val newVrt = buildVrt(z, getImagePaths(workingDir, z, theseCells))
      (newVrt, theseCells.map { case (x, y) => (x / 2, y / 2) })
    }
  }

  def apply(images: Seq[String], workingDir: String)(implicit sc: SparkContext): Unit = {
    val zoomsToImages =
      images
        .map { path => 
          val (zoom, cells) = getGridCells(path)
          (zoom, cells, path)
         }
        .groupBy(_._1)
        .map { case (zoom, group) => (zoom, (group.map(_._2).reduce(_ ++ _), group.map(_._3))) }
        .toMap

    val zoomRanges = (zoomsToImages.toSeq.map(_._1) :+ 1).sortBy(-_).sliding(2).toSeq

    zoomRanges.foreach { z => println(s"ZOOM RANGE $z") }
    zoomRanges.foldLeft(("", Set[(Int, Int)]())) { (previous, zoomRange) =>
      val (previousVrt, previousCellRequests) = previous
      val z1 = zoomRange(0)
      val z2 = zoomRange(1)
      val (newCells, newImages) = zoomsToImages(z1)
      val cells = previousCellRequests ++ newCells
      val images =  
        if(previousVrt == "") { newImages }
        else { newImages :+ previousVrt }
      println(s"ZOOM RANGE $z1 - $z2 CELLS ${cells.size} IMAGES ${images.size}")

      val vrt = buildVrt(z1, images, initial = true)
      tile(vrt, z1, z2, cells, workingDir)
    }
  }

  val IMAGE_SET = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw"
  val REPROJECTED = "/Users/rob/proj/oam/data/faceoff-reproj"
  def main(args: Array[String]): Unit = {
    implicit val sc = getSparkContext()

//    val workingDir = Files.createTempDirectory(null).toString
    val workingDir = "/Users/rob/proj/oam/data/overzoom-tifs"
    try {
      val paths =
//        new File(IMAGE_SET)
        new File(REPROJECTED)
          .listFiles
          .map(_.getPath)

      println(paths.toSeq)
//      val reprojected = sc.parallelize(paths).map { p => reproject(p, REPROJECTED) }.collect
//      apply(reprojected, workingDir)
      apply(paths, workingDir)
      println(s"DONE.")
    } finally {
//      FileUtils.deleteDirectory(new File(workingDir))
      sc.stop
    }
  }
}
