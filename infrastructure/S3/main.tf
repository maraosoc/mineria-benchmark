locals {
  name = var.project_name
}

resource "aws_s3_bucket" "data" {
  bucket        = var.bucket_name
  force_destroy = true
  tags = {
    Name  = local.name
    Owner = var.owner
  }
}

resource "random_id" "server" {

  byte_length = 8
}

{
  tags = {
    Name  = local.name
    Owner = var.owner
  }
}

output "bucket_name" { value = aws_s3_bucket.data.bucket }