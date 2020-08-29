// configure AWS (using credentials file)
provider "aws" {
  shared_credentials_file = "~/.aws/credentials"
  region  = "us-east-1"
  version = "~> 2.0"
}

// configure VPC
resource "aws_vpc" "final_vpc" {
  cidr_block = "10.0.0.0/16"
  tags = {
    Name = "final-vpc"
  }
}

// adding 2 sub-nets to the vpc
// ELB, RDS and servers would use them to communicate
// #1
resource "aws_subnet" "subnet1" {
  vpc_id     = "${aws_vpc.final_vpc.id}"
  cidr_block = "10.0.48.0/20"
  availability_zone = "us-east-1a"
  tags = {
    Name = "subnet1"
  }
}
// #2
resource "aws_subnet" "subnet2" {
  vpc_id     = "${aws_vpc.final_vpc.id}"
  cidr_block = "10.0.32.0/20"
  availability_zone = "us-east-1b"
  tags = {
    Name = "subnet2"
  }
}

// configure internet gateway (required by ELB, as it's inside a VPC)
resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = "${aws_vpc.final_vpc.id}"
}

// configure ELB's security group
resource "aws_security_group" "elb-security-group" {
  name        = "elb-security-group"
  vpc_id      = "${aws_vpc.final_vpc.id}"
  egress {
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    from_port   = 0
    to_port     = 0
  }
  description = "Enbale access to servers to ELB"
  tags = {
    Name = "elb-security-group"
  }
}

// configure servers' security group
resource "aws_security_group" "servers" {
  name        = "servers"
  vpc_id      = "${aws_vpc.final_vpc.id}"
  ingress {
    protocol    = "tcp"
    from_port   = 80  // http port
    to_port     = 80
  security_groups = ["${aws_security_group.elb-security-group.id}"]  // ELB's traffic
    description = "Enable ELBs traffic"
  }
  egress {
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    from_port   = 0
    to_port     = 0
  }
  description = "Enable access to servers"
  tags = {
    Name = "servers"
  }
}

// Standard ELB (w/ basic TCP, HTTP)
resource "aws_elb" "final-elb" {
  name               = "final-elb"

  health_check {
    target              = "HTTP:5000/"
    timeout             = 15
    interval            = 60
    healthy_threshold   = 3
    unhealthy_threshold = 3
  }

  listener {
    lb_protocol       = "http"
    instance_protocol = "http"
    lb_port           = 80
    instance_port     = 5000
  }

  // subnets
  subnets = ["${aws_subnet.subnet1.id}","${aws_subnet.subnet2.id}"]

  // security groups
  security_groups = ["${aws_security_group.elb-security-group.id}"]

  // connection draining
  connection_draining         = true
  connection_draining_timeout = 500

  // LB settings
  cross_zone_load_balancing   = true

  // Idle timeout settings
  idle_timeout                = 500

  tags = {
    Name = "final-elb"
  }
}

// set auto scaling group's launch configuration
resource "aws_launch_configuration" "final_launch_config" {
  name          = "final_launch_config"
  instance_type = "t2.micro"
  image_id      = "ami-058b8fd59d0123a7d" // Linux AMI
  security_groups = ["${aws_security_group.servers.id}"]
}

// configure auto scaling group
// connect LB to it
resource "aws_autoscaling_group" "final_auto_scaling_group" {
  name = "final_auto_scaling_group"

  // set in 2 AZ (same region)
  vpc_zone_identifier = ["${aws_subnet.subnet1.id}","${aws_subnet.subnet2.id}"]
  launch_configuration = "${aws_launch_configuration.final_launch_config.id}"

  // connect LB
  load_balancers = ["${aws_elb.final-elb.id}"]

  // auto scaling group's settings
  min_size           = 1
  max_size           = 4
  desired_capacity   = 1
}

resource "aws_autoscaling_policy" "final_auto_scaling_policy_cpu" {
  name = "final_auto_scaling_policy_cpu"
  autoscaling_group_name = "${aws_autoscaling_group.final_auto_scaling_group.name}"
  policy_type = "TargetTrackingScaling"

  // set tracking metrics and values
  target_tracking_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ASGAverageCPUUtilization"
    }
    // trigger threshold
    target_value = 75.0
  }
}
