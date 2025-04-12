output "redshift_endpoint" {
  description = "The endpoint of the Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.redshift_workgroup.endpoint
}
