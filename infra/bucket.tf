resource "aws_s3_bucket" "esgi_spark" {
  bucket = "esgi-4iabd2-spark-15"

  tags = local.tags
}