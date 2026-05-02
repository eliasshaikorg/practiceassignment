# 1. Define the IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "role-glue-rev-ext-search"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# 2. Attach the standard Glue Service Policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 3. Grant Glue access to S3 script and data buckets
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "GlueS3AccessPolicy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:Get*"
        ]
        Resource = "arn:aws:s3:::s3-glue-scripts-application-dev-bucket/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = "arn:aws:s3:::s3-glue-scripts-application-dev-bucket"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:Put*",
          "s3:Delete*"
        ]
        Resource = [
          "arn:aws:s3:::s3-rawdev-bucket-${data.aws_caller_identity.current.account_id}/*",
          "arn:aws:s3:::s3-conformeddev-bucket-${data.aws_caller_identity.current.account_id}/*",
          "arn:aws:s3:::s3-curateddev-bucket-${data.aws_caller_identity.current.account_id}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::s3-rawdev-bucket-${data.aws_caller_identity.current.account_id}",
          "arn:aws:s3:::s3-conformeddev-bucket-${data.aws_caller_identity.current.account_id}",
          "arn:aws:s3:::s3-curateddev-bucket-${data.aws_caller_identity.current.account_id}"
        ]
      }
    ]
  })
}

# 4. Upload your script to S3
resource "aws_s3_object" "glue_script" {
  bucket = "s3-glue-scripts-application-dev-bucket"
  key    = "glue/scripts/gj-rev-ext-search.py"
  source = "scripts/gj-rev-ext-search.py" # Local path to your script
  source_hash = filebase64sha256("scripts/gj-rev-ext-search.py")
}

# 4. Define the Glue Job
resource "aws_glue_job" "externalrevenue" {
  name     = "gj-rev-ext-search"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
}

# 5. Upload sample data  to S3 raw 
resource "aws_s3_object" "sample-data" {
  bucket = "s3-rawdev-bucket-${data.aws_caller_identity.current.account_id}"
  key    = "externalclickdata/inputdata.sql"
  source = "data/inputdata.sql"
}
