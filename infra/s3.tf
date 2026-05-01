# Create the S3 Bucket
resource "aws_s3_bucket" "cicd-bucket" {
  bucket = "s3-terraform-cicd-bucket-dev"

  tags = {
    Name        = "s3-terraform-cicd-bucket-dev"
    Environment = "Dev"
  }
}

# Create the S3 Bucket
resource "aws_s3_bucket" "gluescripts" {
  bucket = "s3-glue-scripts-application-dev-bucket"

  tags = {
    Name        = "s3-glue-scripts-application-dev-bucket"
    Environment = "Dev"
  }
}

# Create the S3 Bucket for datalake rawzone
resource "aws_s3_bucket" "rawzone" {
  bucket = "s3-raw-zone-dev-bucket"

  tags = {
    Name        = "s3-raw-zone-dev-bucket"
    Environment = "Dev"
  }
}
