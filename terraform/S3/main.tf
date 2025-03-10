resource "aws_s3_bucket" "raw_data" {
  bucket = "dataops-raw-sales-bucket" # Change to a unique name

  #   lifecycle {
  #     prevent_destroy = true
  #   }

  tags = {
    Name        = "Raw Sales Data Bucket"
    Environment = "Development"
  }
}
