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

