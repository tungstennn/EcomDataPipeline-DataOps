resource "aws_redshiftserverless_namespace" "redshift" {
  namespace_name = "dataops-warehouse"
  admin_username = var.admin_username
  admin_user_password = var.admin_user_password
}

resource "aws_redshiftserverless_workgroup" "redshift_workgroup" {
  workgroup_name = "dataops-workgroup"
  namespace_name = aws_redshiftserverless_namespace.redshift.namespace_name
  base_capacity = 8 # Lower capacity keeps costs minimal
}
