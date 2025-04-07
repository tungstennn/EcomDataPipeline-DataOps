resource "aws_iam_role" "redshift_s3_role" {
  name = "RedshiftS3AccessRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "s3_access_policy" {
  name = "S3ReadWriteAccess"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject"
      ]
      Resource = [
        "arn:aws:s3:::ecom-sales-bucket",
        "arn:aws:s3:::ecom-sales-bucket/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role       = aws_iam_role.redshift_s3_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}
