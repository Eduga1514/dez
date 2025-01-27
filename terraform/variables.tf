variable "project" {
    description = "Project"
    default = "dez-bootcamp"
}

variable "location" {
    description = "Project Location"
    default = "US"
}

variable "region" {
    description = "Project Region"
    default = "us-central1"
}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My Storage Bucket Name"
    default = "dez-bootcamp-terraform-bucket"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}