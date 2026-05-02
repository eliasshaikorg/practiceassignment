data "aws_caller_identity" "current" {}

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

# Create the S3 Bucket for datalake raw
resource "aws_s3_bucket" "raw" {
  bucket = "s3-rawdev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-rawdev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}

# Create the S3 Bucket for datalake conformed 
resource "aws_s3_bucket" "conformed" {
  bucket = "s3-conformeddev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-conformeddev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}


# Create the S3 Bucket for datalake curated 
resource "aws_s3_bucket" "curated" {
  bucket = "s3-curateddev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-curateddev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}
