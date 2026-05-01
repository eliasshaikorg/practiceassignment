provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket = "elias-shaik-cicd-bucket"
    key    = "terraform.tfstate" # This is your custom path inside the bucket
    region = "us-east-1"
  }
}
