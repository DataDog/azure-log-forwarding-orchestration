// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

// IgnoredContainers is a list of containers that should be ignored when listing containers for log blobs
var IgnoredContainers = []string{"$logs", "azure-webjobs-hosts", "azure-webjobs-secrets", "dd-forwarder"}

// ForwarderContainer is the container name for the forwarder
const ForwarderContainer = "dd-forwarder"
