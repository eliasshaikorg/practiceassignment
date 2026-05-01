provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket = "s3-terraform-cicd-bucket-dev"
    key    = "terraform.tfstate" # This is your custom path inside the bucket
    region = "us-east-1"
  }
}
