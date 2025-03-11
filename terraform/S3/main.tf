resource "aws_s3_bucket" "raw_data" {
  bucket = "raw-sales-bucket"

  #   lifecycle {
  #     prevent_destroy = true
  #   }

  tags = {
    Name        = "Raw Sales Data Bucket"
    Environment = "Development"
  }
}
