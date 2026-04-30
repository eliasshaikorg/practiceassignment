# Configure the AWS Provider
provider "aws" {
  region = "us-east-1" 
}

# Create the S3 Bucket
resource "aws_s3_bucket" "cicdbucket" {
  bucket = "elias-shaik-cicd-bucket" 

  tags = {
    Name        = "elias-shaik-cicd-bucket"
    Environment = "Dev"
  }
}
