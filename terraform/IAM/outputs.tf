output "iam_role_arn" {
  description = "The ARN of the IAM role for Redshift to access S3"
  value = aws_iam_role.redshift_s3_role.arn
}