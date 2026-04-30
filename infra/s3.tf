# Create the S3 Bucket
resource "aws_s3_bucket" "cicdbucket" {
  bucket = "elias-shaik-cicd-bucket"

  tags = {
    Name        = "elias-shaik-cicd-bucket"
    Environment = "Dev"
  }
}
