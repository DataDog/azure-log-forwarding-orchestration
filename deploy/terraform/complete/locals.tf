locals {
  # Control Plane Configuration
  control_plane_id = var.control_plane_id != null && var.control_plane_id != "" ? var.control_plane_id : random_string.control_plane_id.result

  # Naming Convention
  naming_prefix = "lfo-${local.control_plane_id}"

  # Storage Configuration
  storage_account_name      = "lfostorage${local.control_plane_id}"
  storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.control_plane_storage.name};EndpointSuffix=core.windows.net;AccountKey=${azurerm_storage_account.control_plane_storage.primary_access_key}"

  # Container Images
  deployer_image_url  = "${var.image_registry}/deployer:${var.deployer_image_tag}"
  forwarder_image_url = var.forwarder_image

  # Storage Account URL (auto-generated if not provided)
  storage_account_url = var.storage_account_url != null ? var.storage_account_url : "https://${local.storage_account_name}.blob.core.windows.net/"

  # Resource Names
  resource_names = {
    app_service_plan         = "control-plane-asp-${local.control_plane_id}"
    function_app             = "resources-task-${local.control_plane_id}"
    diagnostic_settings_task = "diagnostic-settings-task-${local.control_plane_id}"
    scaling_task             = "scaling-task-${local.control_plane_id}"
    storage_account          = local.storage_account_name
    file_share               = "resources-task-${local.control_plane_id}"
    cache_container          = "control-plane-cache"
    app_insights             = "control-plane-insights-${local.control_plane_id}"
    log_analytics            = "control-plane-logs-${local.control_plane_id}"
    # Container App Resources  
    container_app_env = "dd-log-forwarder-env-${local.control_plane_id}"
    deployer_task     = "deployer-task-${local.control_plane_id}"
  }

  # Environment Variables for Function App
  common_app_settings = {
    # Azure Function Configuration
    "AzureWebJobsStorage"                      = local.storage_connection_string
    "FUNCTIONS_EXTENSION_VERSION"              = "~4"
    "FUNCTIONS_WORKER_RUNTIME"                 = "python"
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = local.storage_connection_string
    "WEBSITE_CONTENTSHARE"                     = local.resource_names.file_share
    "AzureWebJobsFeatureFlags"                 = "EnableWorkerIndexing"
    "FUNCTIONS_WORKER_PROCESS_COUNT"           = "1"

    # Application Configuration
    "CONTROL_PLANE_ID"        = local.control_plane_id
    "LOG_LEVEL"               = var.log_level
    "MONITORED_SUBSCRIPTIONS" = jsonencode(local.monitored_subscriptions_final)
    "RESOURCE_TAG_FILTERS"    = var.resource_tag_filters

    # Datadog Configuration
    "DD_API_KEY"   = var.datadog_api_key
    "DD_SITE"      = var.datadog_site
    "DD_TELEMETRY" = var.datadog_telemetry ? "true" : "false"

    # Azure Configuration
    "AZURE_CLIENT_ID"       = "" # Will be populated by managed identity
    "AZURE_TENANT_ID"       = "" # Will be populated by managed identity
    "AZURE_SUBSCRIPTION_ID" = data.azurerm_client_config.current.subscription_id
  }

  # Function Configuration
  function_config = {
    schedule = var.resources_task_schedule
    timeout  = 600 # 10 minutes
    memory   = 128 # MB
  }

  # Monitoring Configuration (not used)
  monitoring_config = {
    enabled             = false
    retention_days      = 30
    sampling_percentage = 100
    alert_emails        = []
  }

  # Security Configuration
  security_config = {
    min_tls_version   = "1.2"
    https_only        = true
    ftps_state        = "Disabled"
    remote_debugging  = false
    use_32_bit_worker = var.function_app_use_32_bit_worker
    always_on         = var.function_app_always_on
  }

  # Storage Lifecycle Rules
  storage_lifecycle_rules = [
    {
      name    = "delete-old-cache-blobs"
      enabled = true
      filters = {
        prefix_match = ["${local.resource_names.cache_container}/"]
        blob_types   = ["blockBlob"]
      }
      actions = {
        base_blob = {
          delete_after_days_since_modification_greater_than = var.cache_retention_days
        }
      }
    },
    {
      name    = "delete-old-function-logs"
      enabled = true
      filters = {
        prefix_match = ["azure-webjobs-hosts/"]
        blob_types   = ["blockBlob"]
      }
      actions = {
        base_blob = {
          delete_after_days_since_modification_greater_than = 7
        }
      }
    }
  ]

  # Application Insights Configuration
  # Removed: app_insights_config (no Application Insights integration)

  # Container App Configuration
  container_app_config = {
    # Schedule trigger: every 30 minutes
    schedule_cron_expression = "*/30 * * * *"
    replica_timeout_seconds  = 1800
    replica_retry_limit      = 1
    parallelism              = 1
    replica_completion_count = 1
    # Container resources
    cpu_limit    = 0.5
    memory_limit = "1Gi"
    # Secret names
    connection_string_secret = "connection-string"
    dd_api_key_secret        = "dd-api-key"
  }

  # Tags with defaults
  common_tags = merge(
    var.tags,
    {
      "ManagedBy"      = "terraform"
      "Project"        = "azure-log-forwarding-orchestration"
      "Component"      = "control-plane"
      "ControlPlaneId" = local.control_plane_id
      "ResourceType"   = "control-plane"
    }
  )

  # Management Group Discovery Logic (mimics ARM template logic)
  discovered_subscriptions = var.use_management_group_discovery ? [
    for sub_id in data.azurerm_management_group.target[0].subscription_ids : sub_id
    if !contains(var.excluded_subscriptions, sub_id)
  ] : []

  # Final monitored subscriptions list - use discovered or manual list
  monitored_subscriptions_final = var.use_management_group_discovery ? local.discovered_subscriptions : var.monitored_subscriptions

  # Subscription scope for role assignments
  subscription_scopes = [
    for sub_id in local.monitored_subscriptions_final : "/subscriptions/${sub_id}"
  ]

  # Resource group scope
  resource_group_scope = data.azurerm_resource_group.current.id

  # Function app CORS configuration
  cors_config = {
    allowed_origins     = var.allowed_origins
    allowed_methods     = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    allowed_headers     = ["*"]
    exposed_headers     = ["*"]
    max_age_in_seconds  = 86400
    support_credentials = false
  }

  # Backup configuration
  backup_config = var.enable_backup && var.backup_settings != null ? {
    name                     = var.backup_settings.name
    enabled                  = var.backup_settings.enabled
    storage_account_url      = var.backup_settings.storage_account_url
    frequency_interval       = var.backup_settings.frequency_interval
    frequency_unit           = var.backup_settings.frequency_unit
    retention_period_in_days = var.backup_settings.retention_period_in_days
    keep_at_least_one_backup = true
    start_time               = "2023-01-01T00:00:00Z"
  } : null

  # IP restrictions configuration
  ip_restrictions = concat(
    var.ip_restrictions,
    [
      # Default Azure services access
      {
        service_tag = "AzureCloud"
        name        = "Azure Services"
        priority    = 65000
        action      = "Allow"
      }
    ]
  )

  # SCM IP restrictions configuration
  scm_ip_restrictions = concat(
    var.scm_ip_restrictions,
    [
      # Default Azure DevOps access
      {
        service_tag = "AzureDevOps"
        name        = "Azure DevOps"
        priority    = 65000
        action      = "Allow"
      }
    ]
  )
} 