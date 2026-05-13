# VPC Configuration
resource "aws_vpc" "main" {
    cidr_block           = "10.0.0.0/16"
    enable_dns_hostnames = true
    enable_dns_support  = true

    tags = {
        Name        = "${var.project_name}-vpc"
        Environment = var.environment
    }
}

# Availability Zones data source
data "aws_availability_zones" "available" {
  state = "available"
}

# Public Subnets (For Bastion/DMS/ALB)
resource "aws_subnet" "public" {
    count                   = 2
    vpc_id                  = aws_vpc.main.id
    cidr_block              = "10.0.${count.index + 1}.0/24"
    availability_zone       = data.aws_availability_zones.available.names[count.index]
    map_public_ip_on_launch = true

    tags = {
        Name        = "${var.project_name}-public-subnet-${count.index}"
        Environment = var.environment
    }
}

# Private Subnets (For RDS/Glue?Data Processing)
resource "aws_subnet" "private" {
    count                   = 2
    vpc_id                  = aws_vpc.main.id
    cidr_block              = "10.0.${count.index + 10}.0/24"
    availability_zone       = data.aws_availability_zones.available.names[count.index]
    map_public_ip_on_launch = false

    tags = {
        Name        = "${var.project_name}-private-subnet-${count.index}"
        Environment = var.environment
    }
}

# Gateway & Routing
resource "aws_internet_gateway" "main" {
    vpc_id = aws_vpc.main.id

    tags = {
        Name        = "${var.project_name}-main-igw"
        Environment = var.environment
    }
}

resource "aws_route_table" "public" {
    vpc_id = aws_vpc.main.id

    tags = {
        Name        = "${var.project_name}-public-rt"
        Environment = var.environment
    }
}

resource "aws_route" "public_internet_access" {
    route_table_id         = aws_route_table.public.id
    destination_cidr_block   = "0.0.0.0/0"
    gateway_id               = aws_internet_gateway.main.id
}

resource "aws_route_table" "private" {
    vpc_id = aws_vpc.main.id

    tags = {
        Name        = "${var.project_name}-private-rt"
        Environment = var.environment
    }
}

# Route Table Associations
resource "aws_route_table_association" "public" {
    count          = 2
    subnet_id      = aws_subnet.public[count.index].id
    route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
    count          = 2
    subnet_id      = aws_subnet.private[count.index].id
    route_table_id = aws_route_table.private.id
}

# S3 Gateway Endpoint (The FinOps Flex)
resource "aws_vpc_endpoint" "s3" {
    vpc_id            = aws_vpc.main.id
    service_name      = "com.amazonaws.${var.aws_region}.s3"
    route_table_ids   = [aws_route_table.public.id, aws_route_table.private.id]

    tags = {
        Name        = "${var.project_name}-s3-gateway-endpoint"
        Environment = var.environment
    }
}