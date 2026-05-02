provider "aws" {
  region = "us-east-1"
}

terraform {
  cloud {
    organization = "shaikelias-org"

    workspaces {
      name = "adbeassignment"
    }
  }
}