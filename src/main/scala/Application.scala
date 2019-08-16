import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ObjectMapper
import org.opencb.biodata.models.variant.Variant
import org.opencb.biodata.tools.variant.VariantNormalizer

import collection.JavaConverters._

object Application {
  def main(args: Array[String]) {
    val samples = Seq("s3a://platinum-genomes/2017-1.0/hg38/hybrid/hg38.hybrid.vcf.gz")
    val chromosomes = Seq("1", "2", "16", "17", "18", "19", "20", "21", "22", "MT", "X", "Y")
    val cellbaseAnnotations = chromosomes.map(
      c => "s3a://testingsparkannotations/cellbase_annotations/variation_chr" + c + ".full.json.gz"
    )

    val spark = SparkSession.builder
      .appName("Simple Application")
      .getOrCreate()
    import spark.implicits._

    val vcf = spark.read.textFile(samples: _*)

    val vcfVariants = vcf
        .filter(line => !line.startsWith("#"))
        .map(line => line.split("\t"))
        .filter(tokens => tokens.length > 4)
        .map(tokens => VariantData(tokens(0), tokens(1).toInt, tokens(3), tokens(4)))
        .flatMap(variant => variant.alternate.split(",").map(a => variant.copy(alternate = a)))
        .flatMap(cellbaseNormalisation)

    val cellbaseVariants = spark.read.textFile(cellbaseAnnotations: _*)
      .map(line => AnnotatedVariantData(parseVariantDataFromCellbaseJson(line), line))

    val annotated = vcfVariants.map(v => UnannotatedVariantData(v))
      .join(cellbaseVariants, $"variantData" === $"annotatedVariantData")
      .map(t => (t.get(0).toString,t.get(2).toString))

    val uuid = UUID.randomUUID().toString

    annotated.write.csv("s3a://testingsparkannotations/annotated.csv" + uuid)
  }

  private def parseVariantDataFromCellbaseJson(line: String) = {
    val json = new ObjectMapper().reader().readTree(line)
    VariantData(
      json.get("chromosome").asText(),
      json.get("start").asInt(),
      json.get("reference").asText(),
      json.get("alternate").asText())
  }

  private def cellbaseNormalisation(variant: VariantData) = {
    val config = new VariantNormalizer.VariantNormalizerConfig()
      .setReuseVariants(false)
      .setNormalizeAlleles(false)
      .setDecomposeMNVs(true)
    // hmm, how to do this?
    // .enableLeftAlign(new CellBaseNormalizerSequenceAdaptor(genomeDBAdaptor));

    val normalizer = new VariantNormalizer(config)
    val normalised = normalizer.apply(List(variant.toVariant).asJava)
    normalised.asScala
      .map(variant => VariantData(variant.getChromosome, variant.getStart, variant.getReference, variant.getAlternate))
  }
}


case class VariantData(chromosome: String, position: Int, reference: String, alternate: String){
  def toVariant = new Variant(chromosome, position, reference, alternate)
}

case class AnnotatedVariantData(annotatedVariantData: VariantData, annotations: String)
case class UnannotatedVariantData(variantData: VariantData)
