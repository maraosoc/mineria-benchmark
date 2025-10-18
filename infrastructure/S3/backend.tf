terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "mineria-benchmark-maraosoc-terraform-state"
    key            = "s3/state.tfstate"
    region         = "us-east-2"
    encrypt        = true
    kms_key_id     = "9ddfd080-af73-493a-b58a-e5bb58cab8af"
  }
}

provider "aws" {
  region = "us-east-2"

  default_tags {
    tags = {
      Topic = "terraform"
    }
  }
}
