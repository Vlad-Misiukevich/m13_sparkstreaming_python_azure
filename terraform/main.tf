# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "initial"
    storage_account_name = "initialst"
    container_name       = "initial-container"
    key                  = "***"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  subscription_id = "45a58420-2e1a-4ba1-b23b-c55612222ef5"
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "st${var.ENV}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  network_rules {
    default_action = "Allow"
    ip_rules = values(var.IP_RULES)
  }

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
    azurerm_storage_account.bdcc]

  name = "data"
  storage_account_id = azurerm_storage_account.bdcc.id

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name = "dbw-${var.ENV}-${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  sku = "standard"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

provider "databricks" {
  host = var.databricks_workspace_url
}

variable "databricks_workspace_url" {
  type = string
  default = "https://adb-3243809728100600.0.azuredatabricks.net/?o=3243809728100600#folder/0"
}

resource "databricks_notebook" "bdcc" {
  source     = "C:/Users/Uladzislau_Misiukevi/PycharmProjects/m13_sparkstreaming_python_azure/notebooks/stream.py"
  path       = "/stream"
}

resource "databricks_cluster" "single_node" {
  cluster_name            = "Single Node"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = 20

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

resource "databricks_job" "bdcc" {
  name = "${var.ENV}-job"
  existing_cluster_id = databricks_cluster.single_node.id

  notebook_task {
    notebook_path = databricks_notebook.bdcc.path
  }
}

output "notebook_url" {
  value = databricks_notebook.bdcc.url
}

output "job_url" {
  value = databricks_job.bdcc.url
}