# QA Environment Deployment

## Quick Deploy to QA Environment

For testing and validation purposes, you can deploy the Azure Log Forwarding Orchestration system to our QA environment:

### Complete LFO System (QA)
[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Flfoqa.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Flfoqa.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

### Standalone Forwarder Only (QA)  
[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Flfoqa.blob.core.windows.net%2Ftemplates%2Fforwarder.json)

## Development Resources

- **Control Plane Setup**: See [`control_plane/README.md`](./control_plane/README.md)
- **Forwarder Development**: See [`forwarder/README.md`](./forwarder/README.md)
- **Local Development**: See individual component READMEs for setup instructions

## QA Environment Details

The QA environment provides:
- **Isolated Testing**: Completely separate from production infrastructure
- **Full Feature Parity**: Identical functionality to production deployment
- **Performance Testing**: Load testing capabilities for enterprise-scale validation
- **Integration Testing**: End-to-end testing with real Azure resources

## Support

For QA environment issues or questions, please contact the development team or file an issue in this repository. 