resource "aws_redshiftserverless_namespace" "redshift" {
  namespace_name      = "dataops-warehouse"
  admin_username      = var.admin_username
  admin_user_password = var.admin_user_password
  iam_roles           = [var.iam_role_arn] # IAM role for S3 access
}

resource "aws_redshiftserverless_workgroup" "redshift_workgroup" {
  workgroup_name      = "dataops-workgroup"
  namespace_name      = aws_redshiftserverless_namespace.redshift.namespace_name
  base_capacity       = 8                                   # Lower capacity keeps costs minimal
  security_group_ids  = [aws_security_group.redshift_sg.id] # Reference the security group created below
  publicly_accessible = true
}

data "aws_vpc" "default" {
  default = true
}

# Security group for Redshift
resource "aws_security_group" "redshift_sg" {
  name        = "redshift_sg"
  description = "Allow inbound Redshift traffic"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Allow Redshift port"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # channge to your IP for security
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "redshift_sg"
  }
}

