resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-raw-banking-lake"
  location      = var.region
  force_destroy = true # Allows easy cleanup later if you want to delete the project

  storage_class = "STANDARD"

  # Prevents public access over the internet for security
  public_access_prevention = "enforced"

  # Clean up old data after 30 days automatically
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

