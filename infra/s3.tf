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
  bucket = "s3-raw-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-raw-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}

# Create the S3 Bucket for datalake conformed zone
resource "aws_s3_bucket" "conformedzone" {
  bucket = "s3-conformed-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-conformed-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}


# Create the S3 Bucket for datalake curated zone
resource "aws_s3_bucket" "curatedzone" {
  bucket = "s3-curated-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "s3-curated-zone-dev-bucket-${data.aws_caller_identity.current.account_id}"
    Environment = "Dev"
  }
}
