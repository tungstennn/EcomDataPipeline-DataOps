variable "admin_username" {
  description = "Username for the database"
  type        = string
}

variable "admin_user_password" {
  description = "Password for the database"
  type        = string
}

variable "iam_role_arn" {
  type = string
}