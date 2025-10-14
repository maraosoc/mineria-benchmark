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
    profile        = "ExpertiseBuilding"
    encrypt        = true
    kms_key_id     = "9ddfd080-af73-493a-b58a-e5bb58cab8af"
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
}
