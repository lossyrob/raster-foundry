package com.azavea.foundry.etl

import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.render._
import geotrellis.raster.mosaic._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.tags._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster.reproject._
import geotrellis.spark._
import geotrellis.spark.ingest._
import geotrellis.spark.tiling._
import spire.syntax.cfor._
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.reflect._
import scala.collection.mutable
import sys.process._

case class ImportKey(extent: Extent, zOrder: Int)

case class TileKey(col: Int, row: Int, zOrder: Int)
object TileKey {
  implicit object SpatialComponent extends SpatialComponent[TileKey] {
    def lens =  createLens(k => SpatialKey(k.col, k.row), sk => k => TileKey(sk.col, sk.row, k.zOrder))
  }
}

object Merger {
  def resample(originalExtent: Extent, targetRasterExtent: RasterExtent, tile: MultiBandTile): MultiBandTile = {
    val resampled =
      for(b <- 0 until 3) yield {
        tile.band(b).resample(originalExtent, targetRasterExtent, Bilinear)
      }

    ArrayMultiBandTile(resampled.toArray)

    // val resampled = 
    //   for(b <- 0 until 3) yield {
    //     val tmsTile = ArrayTile.empty(tile.cellType, targetRasterExtent.cols, targetRasterExtent.rows)
    //     tmsTile.merge(targetRasterExtent.extent, originalExtent, tile.band(b))
    //   }

    // ArrayMultiBandTile(resampled.toArray)
  }

  // def merge(e1: Extent, e2: Extent, t1: MultiBandTile, t2: MultiBandTile, z1: Int, z2: Int): MultiBandTile = {
  //   val re1 = RasterExtent(e1, t1.cols, t1.rows)
  //   val re2 = RasterExtent(e2, t2.cols, t2.rows)

  //   val merged =
  //     (for(b <- 0 until 3) yield {
  //       val m = t1.band(b).mutable
  //       val s = t2.band(b)

  //       cfor(0)(_ < t1.rows, _ + 1) { row =>
  //         cfor(0)(_ < t1.cols, _ + 1) { col =>
  //           val v1 = m.get(col, row)
  //           val (scol, srow) = {
  //             val (x, y) = re1.gridToMap(col, row)
  //             re2.mapToGrid(x, y)
  //           }

  //           val v2 = 
  //             if(scol < 0 || srow < 0 || t2.cols <= scol || t2.rows <= srow) { 0 }
  //             else { s.get(scol, srow) }

  //           if(v1 != v2 && v2 != 0) {
  //             if(v1 == 0 || z1 < z2) {
  //               m.set(col, row, v2)
  //             }
  //           }
  //         }
  //       }

  //       m
  //     }).toArray

  //   ArrayMultiBandTile(merged.toArray)
  // }

  def merge(t1: MultiBandTile, t2: MultiBandTile, z1: Int, z2: Int): MultiBandTile = {
    val merged =
      (for(b <- 0 until 3) yield {
        val (m, s) =
          if(z1 < z2) {
            (t2.band(b).mutable, t1.band(b))
          } else {
            (t1.band(b).mutable,  t2.band(b))
          }

        cfor(0)(_ < t1.rows, _ + 1) { row =>
          cfor(0)(_ < t1.cols, _ + 1) { col =>
            val v1 = m.get(col, row)
            val v2 = s.get(col, row)

            if(isNoData(v1)) {
              m.set(col, row, v2)
            }
          }
        }

        // cfor(0)(_ < t1.rows, _ + 1) { row =>
        //   cfor(0)(_ < t1.cols, _ + 1) { col =>
        //     if(v1 != v2 && v2 != 0 && isData(v2)) {
        //       if(v1 == 0 || isNoData(v2) || z1 < z2) {
        //         m.set(col, row, v2)
        //       }
        //     }
        //   }
        // }

        m

//        t1.band(b).merge(t2.band(b))
//        t2.band(b)
      }).toArray

    ArrayMultiBandTile(merged.toArray)
//    t1
  }
}

object Main {
  val TARGET_COLS = 512
  val TARGET_ROWS = 512
  val IMAGE_SET = "/Users/rob/proj/oam/data/sampleset2"

  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[8]")
        .setAppName("Raster Foundry ETL")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.hadoop.KryoRegistrator")

    new SparkContext(conf)
  }

  def getWindows(tags: TiffTags): Seq[(Int, Int, GridBounds)] = {
    val (cols, rows) = (tags.cols, tags.rows)

    val windows = mutable.ListBuffer[(Int, Int, GridBounds)]()
    var tileCol = 0
    cfor(0)(_ < cols, _ + TARGET_COLS) { col =>
      var tileRow = 0
      cfor(0)(_ < rows, _ + TARGET_ROWS) { row =>
        windows += ((tileCol, tileRow, GridBounds(col, row, col + TARGET_COLS - 1, row + TARGET_ROWS - 1)))
        tileRow += 1
      }
      tileCol += 1
    }
    windows.toSeq
  }

  def reproject(path: String, workingDir: String): String = {
    val outPath = (new File(workingDir, new File(path).getName())).getAbsolutePath

    val options = Seq(
      "-q", "-overwrite",
      "-wo", "NUM_THREADS=ALL_CPUS",
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

  def chunk(path: String, tileCol: Int, tileRow: Int, window: GridBounds): String = {
    val script = "/Users/rob/proj/rf/raster-foundry/etl/src/main/python/chunker.py"
    val outputDir = "/Users/rob/proj/rf/raster-foundry/etl/src/main/python/tmp"


    val baseName = {
      val n = new File(path).getName()
      val pos = n.lastIndexOf(".")
      if (pos > 0) {
        n.substring(0, pos)
      } else { n }
    }

    val targetDir = new File(outputDir, baseName)
    targetDir.mkdir()
    val outputPath = new File(targetDir, s"${baseName}_${tileCol}_${tileRow}.tif").getAbsolutePath
    val translateCommand = 
      s"gdal_translate -srcwin ${window.colMin} ${window.rowMin} ${window.width} ${window.height} -q $path $outputPath"
    println(translateCommand)
    val r1 =  translateCommand !

    if(r1 != 0) sys.error(s"Failed: $translateCommand")

    outputPath
  }

  def getRasterMetadata[K](rdd: RDD[(K, MultiBandTile)], crs: CRS, layoutScheme: LayoutScheme)
                (getExtent: K => Extent): (LayoutLevel, RasterMetaData) = {
    val (uncappedExtent, cellType, cellSize): (Extent, CellType, CellSize) =
        envelopeExtent(rdd)(getExtent)

    val worldExtent = crs.worldExtent
//    val layoutLevel: LayoutLevel = layoutScheme.levelFor(worldExtent, cellSize)
    val layoutLevel: LayoutLevel = layoutScheme.levelFor(14)
    val extentIntersection = worldExtent.intersection(uncappedExtent).get
    layoutLevel -> RasterMetaData(cellType, extentIntersection, crs, layoutLevel.tileLayout)
  }

  def envelopeExtent[T](rdd: RDD[(T, MultiBandTile)])(getExtent: T => Extent): (Extent, CellType, CellSize) = {
    rdd
      .map { case (key, tile) =>
        val extent = getExtent(key)
        (extent, tile.cellType, CellSize(extent, tile.cols, tile.rows))
      }
      .reduce { (t1, t2) =>
        val (e1, ct1, cs1) = t1
        val (e2, ct2, cs2) = t2
        (
          e1.combine(e2),
          ct1.union(ct2),
          if (cs1.resolution < cs2.resolution) cs1 else cs2
        )
      }
  }

  def cutTiles[T, K: SpatialComponent: ClassTag] (
    getExtent: T => Extent,
    createKey: (T, SpatialKey) => K,
    rdd: RDD[(T, MultiBandTile)],
    mapTransform: MapKeyTransform,
    cellType: CellType,
    tileLayout: TileLayout
  ): RDD[(K, MultiBandTile)] =
    rdd
      .flatMap { tup =>
        val (inKey, tile) = tup
        val extent = getExtent(inKey)
        mapTransform(extent)
          .coords
          .map  { spatialComponent =>
            val outKey = createKey(inKey, spatialComponent)
            (outKey, Merger.resample(extent, RasterExtent(mapTransform(outKey), tileLayout.tileCols, tileLayout.tileRows), tile))
          }
       }

  def up[K: SpatialComponent: ClassTag](metaData: RasterMetaData, rdd: RDD[(K, MultiBandTile)], level: LayoutLevel, layoutScheme: LayoutScheme): (RasterMetaData, RDD[(K, MultiBandTile)], LayoutLevel) = {
    val nextLevel = layoutScheme.zoomOut(level)
    val nextMetaData =
      RasterMetaData(
        metaData.cellType,
        metaData.extent,
        metaData.crs,
        nextLevel.tileLayout
      )

    // Functions for combine step
    def createTiles(tile: (K, MultiBandTile)): Seq[(K, MultiBandTile)] =
      Seq(tile)

    def mergeTiles1(tiles: Seq[(K, MultiBandTile)], tile: (K, MultiBandTile)): Seq[(K, MultiBandTile)] =
      tiles :+ tile

    def mergeTiles2(tiles1: Seq[(K, MultiBandTile)], tiles2: Seq[(K, MultiBandTile)]): Seq[(K, MultiBandTile)] =
      tiles1 ++ tiles2
    
    val firstMap: RDD[(K, (K, MultiBandTile))] =
      rdd
        .map { case (key, tile: MultiBandTile) =>
          val extent = metaData.mapTransform(key)
          val newSpatialKey = nextMetaData.mapTransform(extent.center)
          (key.updateSpatialComponent(newSpatialKey), (key, tile))
      }

    val combined =
      firstMap
        .combineByKey(createTiles, mergeTiles1, mergeTiles2)

    val nextRdd: RDD[(K, MultiBandTile)] =
      combined.map { case (newKey: K, seq: Seq[(K, MultiBandTile)]) =>
        // val arr = seq.toArray

        // val merged =
        //   for(b <- 0 until 3) yeild {

        //   }

        // if(arr.length > 0) {
        //   val newExtent = nextMetaData.mapTransform(newKey)
        //   arr.reduce { ((k1, t2), (k2, t2)) =>

        //   }
        // } else {
        //   arr(0)
        // }

        val key = seq.head._1

        val newExtent = nextMetaData.mapTransform(newKey)
        val merged =
          (for(b <- 0 until 3) yield {
            ArrayTile.empty(nextMetaData.cellType, nextMetaData.tileLayout.tileCols, nextMetaData.tileLayout.tileRows)
          }).toArray

        for( (oldKey, tile) <- seq) {
          val oldExtent = metaData.mapTransform(oldKey)
            (for(b <- 0 until 3) yield {
              merged(b).merge(newExtent, oldExtent, tile.band(b).resample(Extent(0.0, 0.0, 1.0, 1.0), tile.cols / 2, tile.rows / 2, Bilinear))
            }).toArray
        }

        (newKey, ArrayMultiBandTile(merged): MultiBandTile)
      }

    (nextMetaData, nextRdd, nextLevel)
  }

  def sink(rdd: RDD[(SpatialKey, MultiBandTile)], level: LayoutLevel): Unit = {
    val baseDir = new File(s"/Users/rob/proj/oam/data/tiles2/${level.zoom}")

    rdd.foreach { case (key, tile) =>
      val f = new File(baseDir.getAbsolutePath, s"${key.col}/${key.row}.png")
      f.getParentFile.mkdirs()

      val rgb =
        tile.convert(TypeInt).combine(0, 1, 2) { (r, g, b) =>
          val a = if(isData(r)) 0xFF else 0x00
          (r << 24) | (g << 16) | (b << 8) | a
        }

      rgb.renderPng.write(f.getAbsolutePath)
    }
  }

  def sinkLevels(meta: RasterMetaData, rdd: RDD[(SpatialKey, MultiBandTile)], level: LayoutLevel, layoutScheme: LayoutScheme)(free: => Unit): Unit = {
    println(s"Saving $level")
    if (level.zoom >= 1) {
      rdd.persist(storage.StorageLevel.MEMORY_ONLY_SER)
      sink(rdd, level)
      free
      val (nextMeta, nextRdd, nextLevel) = up(meta, rdd, level, layoutScheme)
      // we must do it now so we can unerspist the source before recurse
      sinkLevels(nextMeta, nextRdd, nextLevel, layoutScheme){ rdd.unpersist(blocking = false) }
    } else {
      sink(rdd, level)
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    val workingDir = Files.createTempDirectory(null).toString
    try {
      val paths =
        new File(IMAGE_SET)
          .listFiles
          .map(_.getPath)

      val preprocessed =
        sc.parallelize(paths)
          .map { path => reproject(path, workingDir) }
          .flatMap { path =>
            getWindows(TiffTagsReader.read(path)).map((path, _))
           }
          .repartition(8)
          .map { case (path, (tileCol, tileRow, window)) => chunk(path, tileCol, tileRow, window) }
          .map { path => 
            // Establish some ordering
            if(path.contains("LC")) {
              if(path.contains("RGB")) { (1, path) }
              else { (0, path) }
            } else {
              (new File(path).getName().take(8).toInt, path)
            }
           }
          .collect

      // preprocessed.map { case (k, v) => k }.distinct.foreach(println)
      // sys.error("done")

      // println(s"COUNT ${preprocessed.size}")
      // sys.error("done")
      val chunked =
        sc.parallelize(preprocessed)
          .repartition(200)
          .map { case (zOrder, path) =>
            val gt = MultiBandGeoTiff(path).mapTile { tile =>
              val converted = tile.convert(TypeShort)
              val nds = 
                if(tile.bandCount == 3) {
                  converted.combine(0, 1, 2) { (r, g, b) =>
                    if(r == 0 && g == 0 && b == 0) NODATA
                    else 1
                  }
                } else {
                  converted.combine(0, 1, 2, 3) { (r, g, b, a) =>
                    if(a != 255.toByte) { NODATA }
                    else {
                      if(r == 0 && g == 0 && b == 0) NODATA
                      else 1
                    }
                  }
                }

              val b0 = converted.band(0).map { (col, row, z) =>
                if(isNoData(nds.get(col, row))) NODATA
                else { if(isNoData(z)) 128 else z.toByte & 0xFF }
              }

              val b1 = converted.band(1).map { (col, row, z) =>
                if(isNoData(nds.get(col, row))) NODATA
                else { if(isNoData(z)) 128 else z.toByte & 0xFF }
              }

              val b2 = converted.band(2).map { (col, row, z) =>
                if(isNoData(nds.get(col, row))) NODATA
                else { if(isNoData(z)) 128 else z.toByte & 0xFF }
              }
              ArrayMultiBandTile(b0, b1, b2)
            }
            // val projectedBands =
            //   for(b <- 0 until gt.bandCount) yield {
            //     gt.tile.band(b).reproject(gt.extent, gt.crs, WebMercator)
            //   }
            // val tiles = projectedBands.map(_.tile)
            // val extent = projectedBands.head.extent

            // (ProjectedExtent(extent, WebMercator), ArrayMultiBandTile(tiles.toArray): MultiBandTile)
            (ImportKey(gt.extent, zOrder), gt.tile)
           }
          
      val layoutScheme = ZoomedLayoutScheme(256)

      val (layoutLevel, metadata) =
        getRasterMetadata(chunked, WebMercator, layoutScheme)(_.extent)

      println(s"$layoutLevel $metadata")
      
      val cut = 
        cutTiles({ ik: ImportKey => ik.extent }, { (ik: ImportKey, k: SpatialKey) => TileKey(k.col, k.row, ik.zOrder) }, chunked, metadata.mapTransform, metadata.cellType, metadata.tileLayout)
          .map { case (k, t) => (SpatialKey(k.col, k.row), (k.zOrder, t)) }
          .groupByKey
          .map { case (k, tiles) =>
            val ordered = tiles.toSeq.sortBy { case (z, t) => -z }.map(_._2).toArray

            val first = ordered(0)
            val bands = Array(first.band(0), first.band(1), first.band(2))

            val merged =
              (for(b <- 0 until 3) yield {
                val m = bands(b).mutable
                for(i <- 1 until ordered.size) {
                  val s = ordered(i).band(b)
                  cfor(0)(_ < first.rows, _ + 1) { row =>
                    cfor(0)(_ < first.cols, _ + 1) { col =>
                      val v1 = m.get(col, row)
                      val v2 = s.get(col, row)

                      if(isNoData(v1)) {
                        m.set(col, row, v2)
                      }
                    }
                  }
                }

                m
              }).toArray

            (k, ArrayMultiBandTile(merged.toArray): MultiBandTile)
        }


          // .reduceByKey { case ((z1, t1), (z2, t2)) =>
          //   (if(z1 > z2) z1 else z2, Merger.merge(t1, t2, z1, z2))
          //  }
          // .map { case (k, (_, t)) => (k, t) }



          // .map { case (k, t) => (SpatialKey(k.col, k.row), t) }
          // .reduceByKey { case (tile1: MultiBandTile, tile2: MultiBandTile) =>
          //   val merged =
          //     for(b <- 0 until tile1.bandCount) yield {
          //       tile1.band(b).merge(tile2.band(b))
          //     }

          //   ArrayMultiBandTile(merged.toArray): MultiBandTile
          //  }


      sinkLevels(metadata, cut, layoutLevel, layoutScheme) { Unit }

      println(s"DONE.")
    } finally {
      FileUtils.deleteDirectory(new File(workingDir))
      sc.stop
    }
  }
}
