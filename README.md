# Automated Log Forwarding for Azure

> *Zero-maintenance log forwarding from Azure to Datadog with intelligent scaling*

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

## Why Automated Log Forwarding for Azure?

**Stop wrestling with Azure log complexity.** Automated Log Forwarding for Azure is the industry's first fully automated, enterprise-scale log forwarding orchestration system that discovers, configures, and forwards **all** your Azure logs without manual intervention.

### 🎯 **Built for Enterprise Scale**
- **Automatic Resource Discovery**: Finds and configures log forwarding for your Azure resources
- **Intelligent Auto-Scaling**: Dynamically provisions forwarders based on actual log volume - scale from zero to millions of logs/second
- **Zero Configuration**: Deploy once, forward forever - no per-resource setup required
- **Enterprise Reliability**: Built-in dead letter queues, cursor-based state management, and automatic retry logic

### 🛡️ **Security & Compliance First**
- **PII Scrubbing**: Configurable data privacy protection removes sensitive information before forwarding
- **Azure-Native Security**: Leverages Azure RBAC, Managed Identity, and encryption at rest

### ⚡ **Performance at Scale**
- **Multi-Region**: Deploys forwarders close to data sources for minimal latency
- **Efficient Batching**: Optimized for Datadog API limits with intelligent compression
- **Resource Optimization**: Pay only for what you use with serverless Container Apps

## Architecture

Automated Log Forwarding for Azure uses a sophisticated three-tier architecture designed for enterprise reliability and performance:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Control Plane  │────│    Forwarders    │────│     Datadog     │
│  (Orchestrator) │    │ (Log Processors) │    │   (Destination) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
   ┌─────▼─────┐            ┌────▼───────┐          ┌────▼────┐
   │ Resource  │            │ Azure      │          │ Datadog │
   │ Discovery │            │ Blob       │          │ Logs    │
   │           │            │ Storage    │          │ API     │
   │ Scaling   │            │ Processing │          └─────────┘
   │           │            └────────────┘
   │ Diagnostic│
   │ Settings  │
   └───────────┘
```


### **Control Plane** (Python)
Intelligent orchestration engine that manages the entire system lifecycle:
- **Resource Discovery**: Continuously scans Azure subscriptions for log-generating resources
- **Smart Scaling**: Provisions/deprovisions forwarders based on real-time log volume metrics
- **Configuration Management**: Automatically configures Azure diagnostic settings
- **Health Monitoring**: Monitors system health and triggers automatic remediation
- **Selective Targeting**: Configurable filters for specific resource types or tags

### **Forwarders** (Go)
High-performance log processing engines optimized for throughput:
- **Stream Processing**: Real-time processing of Azure blob storage streams
- **Multi-Format Parsing**: Native support for all Azure log formats (Function Apps, NSG Flow Logs, Active Directory, etc.)
- **State Management**: Cursor-based tracking ensures zero data loss
- **Error Handling**: Sophisticated retry logic with dead letter queues

### **Integration Layer**
- **Azure-Native**: Uses Azure Container Apps, Storage Accounts, and Managed Identity
- **Datadog Integration**: Optimized for Datadog's ingestion APIs with proper batching and compression

## Quick Start

### One-Click Deployment
Deploy the complete system to your Azure subscription in under 5 minutes:

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

### Standalone Forwarder
Deploy just the forwarder component for specific use cases:

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fforwarder.json)

**That's it.** Automated Log Forwarding for Azure will automatically:
1. Discover all log-generating resources in your subscription(s)
2. Configure diagnostic settings to route logs to storage
3. Deploy and scale forwarders based on log volume
4. Begin forwarding logs to Datadog

Please see our [official documentation](https://docs.datadoghq.com/logs/guide/azure-automated-log-forwarding/) for a more detailed getting started guide.

## Technical Specifications

### **Supported Log Types**
- Function App Logs
- Network Security Group Flow Logs
- Azure Active Directory Logs
- Application Insights Logs
- Resource Manager Activity Logs
- Custom Application Logs
- *And ANY other Azure log types available via Diagnostic Settings*

### **Deployment Options**
- **Full System**: Complete orchestration with auto-discovery
- **Standalone Forwarder**: Individual forwarder instances

## Development & Customization

Automated Log Forwarding for Azure is built with extensibility in mind:

- **🐍 Python Control Plane**: Extensible orchestration logic with comprehensive APIs
- **⚡ Go Forwarders**: High-performance processing with plugin architecture
- **🏗️ Infrastructure as Code**: Complete ARM/Bicep templates for customization
- **🔧 Configuration Management**: YAML-based configuration for all components

**Development Resources:**
- [Control Plane Development](./control_plane/README.md)
- [Forwarder Development](./forwarder/README.md)

## Why Choose Automated Log Forwarding for Azure?

| Traditional Approach | Automated Log Forwarding for Azure |
|---------------------|-----------|
| ❌ Manual configuration per resource | ✅ Automatic discovery and configuration |
| ❌ Fixed scaling, over-provisioning | ✅ Dynamic scaling based on actual usage |
| ❌ Complex maintenance and updates | ✅ Self-managing with automatic updates |

---

**Ready to transform your Azure logging strategy?**
[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

*Built with ❤️ by the Azure Integrations team at Datadog*
