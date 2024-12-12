targetScope = 'subscription'
param controlPlaneResourceGroup string
param controlPlaneLocation string

resource resourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: controlPlaneResourceGroup
  location: controlPlaneLocation
}
