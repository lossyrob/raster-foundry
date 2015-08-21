package com.azavea.foundry.server

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.S3Client._
import geotrellis.vector._
import geotrellis.vector.io.json._
import geotrellis.vector.reproject._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.proj4._

import spire.syntax.cfor._

import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.http.{MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing._
import spray.util.LoggingContext
import spray.http.StatusCodes._

import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}
import com.amazonaws.auth._

case class Options(rootPath: String = "/Users/rob/proj/oam/overzoom/", port: Int = 8088, appDir: String = "/Users/rob/proj/rf/raster-foundry/app")
//case class Options(rootPath: String = "/Users/rob/proj/oam/data/nepal", port: Int = 8088, appDir: String = "/Users/rob/proj/rf/raster-foundry/app")
//case class Options(rootPath: String = "/home/ubuntu/efs/nepal", port: Int = 80, appDir: String = "/home/ubuntu/app")

object TMSService extends App with SimpleRoutingApp {
  val serviceOptions = Options()
  implicit val system = ActorSystem("foundry-server-system")

  val id = "AKIAIRLJSL7HOS6ZI63Q"
  val key = "scmlh4XJt7ifj8NBA9JEN3x/QNfsO7VCgRaFesSF"
  val credentials = new BasicAWSCredentials(id, key)
  val s3Client = new com.amazonaws.services.s3.AmazonS3Client(credentials)


  val layers = List(
    ("356f564e3a0dc9d15553c17cf4583f21-12", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-9", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-24", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-20", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-2", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-7", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-15", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-23", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-8", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-25", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-14", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-26", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-21", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-19", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-22", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-13", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-3", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-1", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-5", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-11", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-16", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-17", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-4", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-6", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-10", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-18", 18),
    ("356f564e3a0dc9d15553c17cf4583f21-0", 18),
    ("LC81410412014277LGN00_bands_432", 12),
    ("LC81410412014293LGN00_bands_432", 12),
    ("LC81410412015088LGN00_bands_432", 12),
    ("LC81410412015120LGN00_bands_432", 12),
    ("LC81420412015111LGN00_bands_432", 12)
  )

  // val layers = List(
  //   ("20140903_154221_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154222_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154223_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154224_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154225_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154226_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154227_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154228_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154229_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154230_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154231_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154232_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154233_0906_visual-reprojected-correctbands", 15),
  //   ("20140903_154239_0906_visual-reprojected-correctbands", 15),
  //   ("20150424_182320_081f_visual-reprojected-correctbands", 15),
  //   ("20150506_134129_0822_visual-reprojected-correctbands", 15),
  //   ("20150514_151301_0822_visual-reprojected-correctbands", 15),
  //   ("20150514_151311_0822_visual-reprojected-correctbands", 15),
  //   ("20150608_160416_0906_visual-reprojected-correctbands", 15),
  //   ("20150609_155942_0905_visual-reprojected-correctbands", 15),
  //   ("20150609_155943_0905_visual-reprojected-correctbands", 15),
  //   ("20150609_155945_0905_visual-reprojected-correctbands", 15),
  //   ("20150609_155946_0905_visual-reprojected-correctbands", 15),
  //   ("20150609_155947_0905_visual-reprojected-correctbands", 15),
  //   ("20150609_155949_0905_visual-reprojected-correctbands", 15),
  //   ("20150624_160324_0905_visual-reprojected-correctbands", 15),
  //   ("LC80200282014245LGN00_bands_432-reprojected-correctbands", 12),
  //   ("LC80210282014156LGN00_RGB-reprojected-correctbands", 12),
  //   ("LC80220282015102LGN01_bands_432-reprojected-correctbands", 12)
  // )

  val mapTransforms: Function1[Int, MapKeyTransform] = {
    val worldExtent = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244)

    //println(f"WORLD EXTENT: ${worldExtent.xmin}%f ${worldExtent.ymin}%f ${worldExtent.xmax}%f ${worldExtent.ymax}%f")
    (1 to 18)
      .map { z =>
        val layoutCols = math.pow(2, z).toInt
        val layoutRows = layoutCols
        (z, MapKeyTransform(worldExtent, layoutCols, layoutRows))
       }
      .toMap
  }

  val bucket = "oamtiler-test"

  // S3 (not working)
  // def tmsRoute =
  //   pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //     respondWithMediaType(MediaTypes.`image/png`) {
  //       val paths =
  //         layers
  //           .flatMap { case (layer, maxZoom) =>
  //             val (lz, lx, ly) = {
  //               if(zoom <= maxZoom)
  //                 (zoom, x, y)
  //               else {
  //                 val d = math.pow(2, zoom - maxZoom)
  //                 (maxZoom, (x / d).toInt, (y / d).toInt)
  //               }
  //             }
              
  //             val key = s"$layer/$lz/$lx/$ly.tiff"
  //             try {
  //               val gt = MultiBandGeoTiff(s3Client.getObject(bucket, key).toBytes)
  //               Some((gt, lz, lx, ly))
  //             } catch {
  //               case e: com.amazonaws.services.s3.model.AmazonS3Exception =>
  //                 None
  //             }
  //           }

  //       if(paths.isEmpty) {
  //         reject
  //       } else {
  //         complete {
  //           Future {
  //             paths.foreach(println)
  //             val ordered =
  //               paths.map { case (gt, lz, lx, ly) =>

  //                 if(zoom == lz)
  //                   gt.tile.convert(TypeInt)
  //                 else {
  //                   val t = gt.tile
  //                   val re = RasterExtent(mapTransforms(zoom)(x, y), 256, 256)
  //                   val arr = Array.ofDim[Tile](t.bandCount)
  //                   cfor(0)(_ < 3, _ + 1) { b =>
  //                     arr(b) = t.band(b).convert(TypeInt).map { z => if(isNoData(z)) 128 else z.toByte & 0xFF }.resample(gt.extent, re, Bilinear)
  //                   }

  //                   ArrayMultiBandTile(arr)
  //                 }
  //                 }
  //               .toArray

  //             val rgb = IntArrayTile(Array.ofDim[Int](256 * 256), 256, 256)

  //             cfor(1)(_ < ordered.size, _ + 1) { i =>
  //               val image = ordered(i)
  //               val i1 = image.band(0)
  //               val i2 = image.band(1)
  //               val i3 = image.band(2)

  //               cfor(0)(_ < 256, _ + 1) { row =>
  //                 cfor(0)(_ < 256, _ + 1) { col =>
  //                   var v = 0
  //                   var i = 0
  //                   while(v == 0 && i < ordered.size) {
  //                     val image = ordered(i)
  //                     val i1 = image.band(0)
  //                     val i2 = image.band(1)
  //                     val i3 = image.band(2)

  //                     v = {
  //                       val r = i1.get(col, row)
  //                       val g = i2.get(col, row)
  //                       val b = i3.get(col, row)
  //                       if(r == 0 && g == 0 && b == 0) 0
  //                       else {
  //                         val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
  //                         val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
  //                         val cb = if(isNoData(b)) 128 else b.toByte & 0xFF

  //                         (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
  //                       }
  //                     }
  //                     i += 1
  //                   }
  //                   rgb.set(col, row, v)
  //                 }
  //               }
  //             }

  //             rgb.renderPng.bytes
  //           }
  //         }
  //       }
  //     }
  //   }


  // def tmsRoute =
  //   pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //     respondWithMediaType(MediaTypes.`image/png`) {
  //       val paths =
  //         layers
  //           .flatMap { case (layer, maxZoom) =>
  //             val (lz, lx, ly) = {
  //               if(zoom <= maxZoom)
  //                 (zoom, x, y)
  //               else {
  //                 val d = math.pow(2, zoom - maxZoom)
  //                 (maxZoom, (x / d).toInt, (y / d).toInt)
  //               }
  //             }
              
  //             val path = s"${serviceOptions.rootPath}/$layer/$lz/$lx/$ly.tiff"
  //             if(!new java.io.File(path).exists) {
  //               None
  //             }
  //             else {
  //               Some((path, lz, lx, ly))
  //             }
  //           }

  //       if(paths.isEmpty) {
  //         reject
  //       } else {
  //         complete {
  //           Future {
  //             paths.foreach(println)
  //             val ordered =
  //               paths.map { case (path, lz, lx, ly) =>
  //                 val gt = MultiBandGeoTiff(path)
  //                 if(zoom == lz)
  //                   gt.tile.convert(TypeInt)
  //                 else {
  //                   val t = gt.tile
  //                   val re = RasterExtent(mapTransforms(zoom)(x, y), 256, 256)
  //                   val arr = Array.ofDim[Tile](t.bandCount)
  //                   cfor(0)(_ < 3, _ + 1) { b =>
  //                     arr(b) = t.band(b).convert(TypeInt).map { z => if(isNoData(z)) 128 else z.toByte & 0xFF }.resample(gt.extent, re, Bilinear)
  //                   }

  //                   ArrayMultiBandTile(arr)
  //                 }
  //                 }
  //               .toArray

  //             val rgb = IntArrayTile(Array.ofDim[Int](256 * 256), 256, 256)

  //             cfor(1)(_ < ordered.size, _ + 1) { i =>
  //               val image = ordered(i)
  //               val i1 = image.band(0)
  //               val i2 = image.band(1)
  //               val i3 = image.band(2)

  //               cfor(0)(_ < 256, _ + 1) { row =>
  //                 cfor(0)(_ < 256, _ + 1) { col =>
  //                   var v = 0
  //                   var i = 0
  //                   while(v == 0 && i < ordered.size) {
  //                     val image = ordered(i)
  //                     val i1 = image.band(0)
  //                     val i2 = image.band(1)
  //                     val i3 = image.band(2)

  //                     v = {
  //                       val r = i1.get(col, row)
  //                       val g = i2.get(col, row)
  //                       val b = i3.get(col, row)
  //                       if(r == 0 && g == 0 && b == 0) 0
  //                       else {
  //                         val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
  //                         val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
  //                         val cb = if(isNoData(b)) 128 else b.toByte & 0xFF

  //                         (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
  //                       }
  //                     }
  //                     i += 1
  //                   }
  //                   rgb.set(col, row, v)
  //                 }
  //               }
  //             }

  //             rgb.renderPng.bytes
  //           }
  //         }
  //       }
  //     }
  //   }

  // //Version that converts to packed int single band per image first.
  // def tmsRoute =
  //   pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //     respondWithMediaType(MediaTypes.`image/png`) {
  //       val paths =
  //         layers
  //           .flatMap { case (layer, maxZoom) =>
  //             val (lz, lx, ly) = {
  //               if(zoom <= maxZoom)
  //                 (zoom, x, y)
  //               else {
  //                 val d = math.pow(2, zoom - maxZoom)
  //                 (maxZoom, (x / d).toInt, (y / d).toInt)
  //               }
  //             }
              
  //             val path = s"${serviceOptions.rootPath}/$layer/$lz/$lx/$ly.tiff"
  //             if(!new java.io.File(path).exists) {
  //               None
  //             }
  //             else {
  //               Some((path, lz, lx, ly))
  //             }
  //           }

  //       if(paths.isEmpty) {
  //         reject
  //       } else {
  //         println(s"NUMBER OF IMAGES FOR TILE $zoom/$x/$y : ${paths.size}")
  //         complete {
  //           Future {
  //             val ordered =
  //               paths.map { case (path, lz, lx, ly) =>
  //                 val converted: MultiBandTile = {
  //                   val gt = MultiBandGeoTiff(path)
  //                   if(zoom == lz)
  //                     gt.tile.convert(TypeInt)
  //                   else {
  //                     val t = gt.tile
  //                     val re = RasterExtent(mapTransforms(zoom)(x, y), 256, 256)
  //                     val arr = Array.ofDim[Tile](t.bandCount)
  //                     cfor(0)(_ < 3, _ + 1) { b =>
  //                       arr(b) = t.band(b).convert(TypeInt).map { z => if(isNoData(z)) 128 else z.toByte & 0xFF }.resample(gt.extent, re, Bilinear)
  //                     }

  //                     ArrayMultiBandTile(arr)
  //                   }
  //                 }

  //                 if(converted.bandCount == 3) {
  //                   converted.combine(0, 1, 2) { (r, g, b) =>
  //                     if(r == 0 && g == 0 && b == 0) 0
  //                     else {
  //                       val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
  //                       val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
  //                       val cb = if(isNoData(b)) 128 else b.toByte & 0xFF

  //                       (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
  //                     }
  //                   }
  //                 } else {
  //                   converted.combine(0, 1, 2, 3) { (r, g, b, a) =>
  //                     if(a != 255.toByte || (r == 0 && g == 0 && b == 0)) {
  //                       0
  //                     } else {
  //                       val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
  //                       val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
  //                       val cb = if(isNoData(b)) 128 else b.toByte & 0xFF

  //                       (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
  //                     }
  //                   }
  //                 }
  //                }
  //               .toArray

  //             val m = ordered(0).mutable
  //             cfor(1)(_ < ordered.size, _ + 1) { i =>
  //               val s = ordered(i)
  //               cfor(0)(_ < 256, _ + 1) { row =>
  //                 cfor(0)(_ < 256, _ + 1) { col =>
  //                   val v1 = m.get(col, row)
  //                   val v2 = s.get(col, row)

  //                   if(v1 == 0) {
  //                     m.set(col, row, v2)
  //                   }
  //                 }
  //               }
  //             }

  //             val rgb = m

  //             rgb.renderPng.bytes
  //           }
  //         }
  //       }
  //     }
  //   }

  // Version that tries to only read the files it needs.
  def tmsRoute =
    pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
      respondWithMediaType(MediaTypes.`image/png`) {
        val paths =
          layers
            .flatMap { case (layer, maxZoom) =>
              val (lz, lx, ly) = {
                if(zoom <= maxZoom)
                  (zoom, x, y)
                else {
                  val d = math.pow(2, zoom - maxZoom)
                  (maxZoom, (x / d).toInt, (y / d).toInt)
                }
              }
              
              val path = s"${serviceOptions.rootPath}/$layer/$lz/$lx/$ly.tiff"
              if(!new java.io.File(path).exists) {
                None
              }
              else {
                Some((path, lz, lx, ly))
              }
            }
            .toArray

        if(paths.isEmpty) {
          reject
        } else {
          complete {
            Future {
              val rgb = IntArrayTile(Array.ofDim[Int](256 * 256), 256, 256)

              val len = paths.length
              var filled = false
              var i = 0
              while(i < len && !filled) {
                val (path, lz, lx, ly) = paths(i)

                val image: MultiBandTile = {
                  val gt = MultiBandGeoTiff(path)
                  if(zoom == lz)
                    gt.tile.convert(TypeInt)
                  else {
                    val t = gt.tile
                    val re = RasterExtent(mapTransforms(zoom)(x, y), 256, 256)
                    val arr = Array.ofDim[Tile](t.bandCount)
                    cfor(0)(_ < 3, _ + 1) { b =>
                      arr(b) = t.band(b).convert(TypeInt).map { z => if(isNoData(z)) 128 else z.toByte & 0xFF }.resample(gt.extent, re, Bilinear)
                    }

                    ArrayMultiBandTile(arr)
                  }
                }

                val i1 = image.band(0)
                val i2 = image.band(1)
                val i3 = image.band(2)

                filled = true
                cfor(0)(_ < 256, _ + 1) { row =>
                  cfor(0)(_ < 256, _ + 1) { col =>
                    if(rgb.get(col, row) == 0) {
                      val v = {
                        val r = i1.get(col, row)
                        val g = i2.get(col, row)
                        val b = i3.get(col, row)
                        if(r == 0 && g == 0 && b == 0) 0
                        else {
                          val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
                          val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
                          val cb = if(isNoData(b)) 128 else b.toByte & 0xFF

                          (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
                        }
                      }
                      if(v != 0) rgb.set(col, row, v)
                      else { filled = false }
                    }
                  }
                }
                if(filled) { println(s"$zoom $x $y filled by $path $lz (image $i of $len)") }
                i += 1
              }

              rgb.renderPng.bytes
            }
          }
        }
      }
    }



  def root = {
    pathPrefix("tms") { tmsRoute } ~
    pathSingleSlash { getFromFile(new java.io.File(serviceOptions.appDir, "index.html").getAbsolutePath) } ~
    getFromDirectory(serviceOptions.appDir)
  }

  startServer(interface = "0.0.0.0", port = serviceOptions.port) {
    root
  }
}
