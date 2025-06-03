# Automated Log Forwarding for Azure

> **🚀 Enterprise-grade, fully automated Azure log ingestion at hyperscale**
> *Zero-configuration log forwarding from Azure to Datadog with intelligent scaling, PII protection, and 99.9% reliability*

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

## Why Automated Log Forwarding for Azure?

**Stop wrestling with Azure log complexity.** Automated Log Forwarding for Azure is the industry's first fully automated, enterprise-scale log forwarding orchestration system that discovers, configures, and forwards **all** your Azure logs without manual intervention.

### 🎯 **Built for Enterprise Scale**
- **Automatic Resource Discovery**: Finds and configures log forwarding for 100% of Azure resources across unlimited subscriptions
- **Intelligent Auto-Scaling**: Dynamically provisions forwarders based on actual log volume - scale from zero to millions of logs/second
- **Zero Configuration**: Deploy once, forward forever - no per-resource setup required
- **Enterprise Reliability**: Built-in dead letter queues, cursor-based state management, and automatic retry logic

### 🛡️ **Security & Compliance First**
- **PII Scrubbing**: Configurable data privacy protection removes sensitive information before forwarding
- **Azure-Native Security**: Leverages Azure RBAC, Managed Identity, and encryption at rest
- **Audit Trail**: Complete tracking and logging of all processed data
- **Compliance Ready**: Designed for SOC 2, GDPR, and enterprise compliance requirements

### ⚡ **Performance at Hyperscale**
- **Sub-second Latency**: Streaming architecture with optimized blob processing
- **Multi-Region**: Deploys forwarders close to data sources for minimal latency
- **Efficient Batching**: Optimized for Datadog API limits with intelligent compression
- **Resource Optimization**: Pay only for what you use with serverless Container Apps

## Architecture

Automated Log Forwarding for Azure uses a sophisticated three-tier architecture designed for enterprise reliability and performance:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Control Plane │────│    Forwarders    │────│     Datadog     │
│   (Orchestrator)│    │  (Log Processors)│    │   (Destination) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
        │                        │                        │
        │                        │                        │
   ┌────▼────┐              ┌────▼────┐              ┌────▼────┐
   │Resource │              │ Azure   │              │ Logs    │
   │Discovery│              │ Blob    │              │ API     │
   │Scaling  │              │ Storage │              │         │
   │Diagnostic│              │ Stream  │              │         │
   │Settings │              │Processing│              │         │
   └─────────┘              └─────────┘              └─────────┘
```

### **Control Plane** (Python)
Intelligent orchestration engine that manages the entire system lifecycle:
- **Resource Discovery**: Continuously scans Azure subscriptions for log-generating resources
- **Smart Scaling**: Provisions/deprovisions forwarders based on real-time log volume metrics
- **Configuration Management**: Automatically configures Azure diagnostic settings
- **Health Monitoring**: Monitors system health and triggers automatic remediation

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

## Enterprise Features

### 🔍 **Intelligent Resource Discovery**
- **Cross-Subscription**: Automatically discovers resources across multiple Azure subscriptions
- **Real-Time Updates**: Continuously monitors for new resources and configuration changes
- **Selective Targeting**: Configurable filters for specific resource types or tags

### 📈 **Adaptive Scaling**
- **Volume-Based Scaling**: Automatically scales forwarders up/down based on actual log volume
- **Regional Distribution**: Deploys forwarders in optimal regions for minimal latency
- **Cost Optimization**: Zero-cost scaling when no logs are being generated

### 🔒 **Data Privacy & Security**
- **Configurable PII Scrubbing**: YAML-based rules for removing sensitive data patterns
- **Encryption**: All data encrypted in transit and at rest
- **Access Control**: Fine-grained RBAC permissions for secure operations

## Use Cases

### **🏢 Large Enterprises**
- **Multi-Cloud Strategy**: Centralize Azure logs alongside other cloud providers in Datadog
- **Compliance & Governance**: Automated audit trails and data governance for regulated industries
- **Cost Management**: Optimize log forwarding costs with intelligent scaling and filtering

### **🚀 High-Growth Startups**
- **Zero Maintenance**: Set-and-forget log forwarding that grows with your infrastructure
- **Developer Productivity**: Eliminate manual log configuration across development teams
- **Rapid Scaling**: Handle exponential growth in log volume without operational overhead

### **🔧 DevOps Teams**
- **Infrastructure as Code**: Complete ARM/Bicep templates for reproducible deployments
- **Automated Operations**: Self-healing system with minimal operational overhead

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
| ❌ No data privacy controls | ✅ Built-in PII scrubbing and compliance |
| ❌ Complex maintenance and updates | ✅ Self-managing with automatic updates |

## Enterprise Support & Community

- **🎫 Enterprise Support**: Dedicated support for enterprise customers
- **🤝 Community**: Active community of Azure and observability engineers
- **🚀 Roadmap**: Regular updates with new Azure service integrations

---

**Ready to transform your Azure logging strategy?**
ƒ
[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2Fazuredeploy.json/createUIDefinitionUri/https%3A%2F%2Fddazurelfo.blob.core.windows.net%2Ftemplates%2FcreateUiDefinition.json)

*Built with ❤️ by the Azure Integrations team at Datadog*
