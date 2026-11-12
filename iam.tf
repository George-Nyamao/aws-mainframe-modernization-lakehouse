# ------------------------------------------------------------------------------
# 1. AWS DMS IAM Role (Least Privilege: Write-only to S3 Raw / Bronze Zone)
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "dms_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "Service"
      identifiers = [
        "dms.amazonaws.com",
        "dms.${var.aws_region}.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_role" "dms_s3" {
  name               = "${var.project_name}-dms-s3-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json

  tags = {
    Name        = "${var.project_name}-dms-s3-role"
    Environment = var.environment
  }
}

data "aws_iam_policy_document" "dms_s3_least_privilege" {
  statement {
    sid    = "AllowListDataLakeBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.datalake.arn
    ]
  }

  statement {
    sid    = "AllowDMSWriteToRawZoneOnly"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:PutObjectTagging",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "${aws_s3_bucket.datalake.arn}/raw/*"
    ]
  }
}

resource "aws_iam_role_policy" "dms_s3" {
  name   = "${var.project_name}-dms-s3-raw-policy"
  role   = aws_iam_role.dms_s3.id
  policy = data.aws_iam_policy_document.dms_s3_least_privilege.json
}

# ------------------------------------------------------------------------------
# 2. AWS Glue IAM Role (Least Privilege: Read Raw/Scripts, Read/Write Iceberg)
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_etl" {
  name               = "${var.project_name}-glue-etl-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = {
    Name        = "${var.project_name}-glue-etl-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_datalake_least_privilege" {
  statement {
    sid    = "AllowListDataLakeBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.datalake.arn
    ]
  }

  statement {
    sid    = "AllowReadFromRawAndScriptsZones"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.datalake.arn}/raw/*",
      "${aws_s3_bucket.datalake.arn}/scripts/*"
    ]
  }

  statement {
    sid    = "AllowReadWriteToIcebergAndTempZones"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "${aws_s3_bucket.datalake.arn}/iceberg*",
      "${aws_s3_bucket.datalake.arn}/silver/*",
      "${aws_s3_bucket.datalake.arn}/gold/*",
      "${aws_s3_bucket.datalake.arn}/temp/*",
      "${aws_s3_bucket.datalake.arn}/athena-results/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_datalake" {
  name   = "${var.project_name}-glue-datalake-policy"
  role   = aws_iam_role.glue_etl.id
  policy = data.aws_iam_policy_document.glue_datalake_least_privilege.json
}
