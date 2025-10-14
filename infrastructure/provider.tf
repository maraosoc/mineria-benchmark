terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
  }

  backend "s3" {
    bucket         = "mineria-benchmark-maraosoc-terraform-state"
    key            = "terraform/state.tfstate"
    region         = "us-east-2"
    dynamodb_table = "TerraformLockTable"
    encrypt        = true
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
}
