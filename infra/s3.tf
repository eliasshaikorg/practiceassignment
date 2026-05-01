# Create the S3 Bucket
resource "aws_s3_bucket" "cicdbucket" {
  bucket = "elias-shaik-cicd-bucket"

  tags = {
    Name        = "s3-terraform-cicd-bucket-dev"
    Environment = "Dev"
  }
}

# Create the S3 Bucket
resource "aws_s3_bucket" "gluescripts" {
  bucket = "glue-scripts-application-dev-bucket"

  tags = {
    Name        = "s3-glue-scripts-application-dev-bucket"
    Environment = "Dev"
  }
}
