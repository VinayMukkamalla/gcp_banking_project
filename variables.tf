variable "project_id" {
  type        = string
  description = "The ID of your GCP Project"
  default     = "gcp-data-engineer-501607" 
}

variable "region" {
  type    = string
  default = "asia-south1" # This is the Mumbai region, closest to you!
}


