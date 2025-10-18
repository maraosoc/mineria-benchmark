variable "prefix" {
  description = "Prefix for the bucket"
  type        = string
  default = "maraosoc"
}


resource "aws_s3_bucket" "create_bucket" {
  bucket        = "${var.prefix}-${random_id.server.hex}"
  force_destroy = true


}

resource "random_id" "server" {

  byte_length = 8
}