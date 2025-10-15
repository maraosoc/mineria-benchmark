locals {
  name = var.project_name
}

# VPC/Subred por defecto
data "aws_vpc" "default" { default = true }
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
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

resource "aws_security_group" "sg" {
  name        = "${local.name}-sg"
  description = "Acceso SSH y salida"
  vpc_id      = data.aws_vpc.default.id
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name  = "${local.name}-sg"
    Owner = var.owner
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "runner" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.sg.id]
  key_name               = var.key_pair_name

#  connection {
#    type        = "ssh"
#    user        = "ubuntu"
#    private_key = file("~/.ssh/mineria-key.pem")
#    host        = self.public_ip
#  }

#  provisioner "remote-exec" {
#    inline = [
#      "echo 'Conexión SSH exitosa desde Terraform'",
#      "hostname"
#    ]
#  }

  tags = {
    Name  = local.name
    Owner = var.owner
  }
}

output "bucket_name" { value = aws_s3_bucket.data.bucket }
output "instance_public_ip" { value = aws_instance.runner.public_ip }
