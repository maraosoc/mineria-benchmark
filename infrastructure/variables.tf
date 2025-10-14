variable "region" {
  type    = string
  default = "us-east-2"
}

variable "profile" {
  type        = string
  description = "Perfil de AWS CLI a usar"
  default     = "maraosoc"
}

variable "owner" {
  type        = string
  description = "Propietario de los recursos"
  default     = "maraosoc"
}

variable "project_name" {
  type    = string
  default = "mineria-benchmark"
}

variable "bucket_name" {
  type = string
}

variable "key_pair_name" {
  type        = string
  description = "Nombre del key pair existente para SSH"
}

variable "instance_type" {
  type    = string
  default = "m5.2xlarge"
}

variable "vpc_id" {
  type        = string
  description = "VPC destino"
}

variable "subnet_id" {
  type        = string
  description = "Subred pública"
}
