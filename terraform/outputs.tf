output "redshift_endpoint" {
  value = module.redshift.redshift_endpoint
}

output "iam_role_arn" {
  value = module.iam.iam_role_arn
}
