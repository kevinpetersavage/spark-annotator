import java.util.UUID

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ObjectMapper
import org.opencb.biodata.models.variant.Variant
import org.opencb.biodata.tools.variant.VariantNormalizer

import collection.JavaConverters._

object FileConvertor {
  def main(args: Array[String]) {
    val chromosomes = Seq("16", "17", "18", "19", "20", "21", "22", "MT", "X", "Y")
    val cellbaseAnnotations = chromosomes.map(
      c => "s3a://testingsparkannotations/cellbase_annotations/variation_chr" + c + ".full.json.gz"
    )

    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.local.dir", "/vol/")
      .getOrCreate()
    import spark.implicits._


    val cellbaseVariants = spark.read.textFile(cellbaseAnnotations: _*).repartition(500)

    val uuid = UUID.randomUUID().toString

    cellbaseVariants.write
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .text("s3a://testingsparkannotations/converted_cellbase_annotations" + uuid)
  }
}