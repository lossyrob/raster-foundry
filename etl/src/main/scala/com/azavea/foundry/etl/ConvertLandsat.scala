package com.azavea.foundry.etl

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._

import sys.process._

object ConvertLandsat {
  // Convert a landsat set of bands into an RGB GeoTiff.
  def main(args: Array[String]): Unit = {
    val d = "/Users/rob/proj/oam/data/tmp"
    val n = "LC80210282014156LGN00"

    def getBandPath(b: Int) = s"${d}/${n}_B${b}.TIF"

    println("Reading red...")
    val rgt = SingleBandGeoTiff(getBandPath(4))
    val crs = CRS.fromName("EPSG:32617")
    val extent = rgt.extent

    val r = rgt.tile.rescale(0, 255).convert(TypeByte)
    println("Reading green...")
    val g = SingleBandGeoTiff(getBandPath(3)).tile.rescale(0, 255).convert(TypeByte)
    println("Reading blue...")
    val b = SingleBandGeoTiff(getBandPath(2)).tile.rescale(0, 255).convert(TypeByte)

    val out = ArrayMultiBandTile(Array(r, g, b))
    val path1 = s"${d}/${n}_RGB-step1.TIF"
    val path2 = s"${d}/${n}_RGB.TIF"

    println("Saving...")
    MultiBandGeoTiff(out, extent, crs).write(path1)

    val options = Seq(
      "-ot", "BYTE",
      "-a_nodata none"
    ).mkString(" ")


    val cmd =
      s"gdal_translate -a_srs EPSG:32617 $options $path1 $path2"
    println(cmd)
    val result =  cmd !

    if(result != 0) sys.error(s"Failed: $cmd")
  }
}
