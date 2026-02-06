# Security Configuration

# Storage Account Role Assignment for Function App
resource "azurerm_role_assignment" "function_app_storage_access" {
  scope                = azurerm_storage_account.control_plane_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.resources_task.identity[0].principal_id
}