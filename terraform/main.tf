terraform {
  required_version = ">= 1.0"
}

module "s3" {
  source = "./S3"
}

module "redshift" {
  source              = "./Redshift"
  admin_username      = var.admin_username
  admin_user_password = var.admin_user_password
  iam_role_arn        = module.iam.iam_role_arn
}

module "iam" {
  source = "./IAM"
}
