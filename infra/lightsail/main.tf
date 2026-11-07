terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

variable "ssh_public_key" {
  type = string
}

variable "admin_cidr" {
  type = string
  validation {
    condition     = can(cidrhost(var.admin_cidr, 0)) && endswith(var.admin_cidr, "/32")
    error_message = "Use one administrator IPv4 address with a /32 prefix."
  }
}

resource "aws_lightsail_key_pair" "bot" {
  name       = "prediction-bot-ireland"
  public_key = var.ssh_public_key
}

resource "aws_lightsail_instance" "bot" {
  name              = "prediction-bot-ie-2gb"
  availability_zone = "eu-west-1a"
  blueprint_id      = "ubuntu_24_04"
  bundle_id         = "small_3_0"
  key_pair_name     = aws_lightsail_key_pair.bot.name
  user_data         = file("${path.module}/bootstrap.sh")
  # Bootstrap runs only at creation; apply later bootstrap fixes over SSH.
  lifecycle {
    ignore_changes = [user_data]
  }
  tags = {
    Project = "prediction-markets"
    Purpose = "2gb-validation"
  }
}

resource "aws_lightsail_instance_public_ports" "bot" {
  instance_name = aws_lightsail_instance.bot.name
  port_info {
    protocol  = "tcp"
    from_port = 22
    to_port   = 22
    cidrs     = [var.admin_cidr]
  }
}

output "public_ip" {
  value = aws_lightsail_instance.bot.public_ip_address
}
