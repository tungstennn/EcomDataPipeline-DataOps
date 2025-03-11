resource "aws_s3_bucket" "raw_data" {
  bucket = "ecom-sales-bucket"

  #   lifecycle {
  #     prevent_destroy = true
  #   }

  tags = {
    Name        = "Sales Data Bucket"
    Environment = "Development"
  }
}
