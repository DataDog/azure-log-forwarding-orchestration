package storage

// IgnoredContainers is a list of containers that should be ignored when listing containers for log blobs
var IgnoredContainers = []string{"$logs", "azure-webjobs-hosts", "azure-webjobs-secrets", "dd-forwarder"}

// ForwarderContainer is the container name for the forwarder
const ForwarderContainer = "dd-forwarder"
