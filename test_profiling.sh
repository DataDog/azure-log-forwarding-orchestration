#!/bin/bash

echo "========================================"
echo "Testing Profiling Implementation"
echo "========================================"

echo ""
echo "1. Checking profiler import in forwarder.go..."
echo "-----------------------------------------------"
if grep -q '"gopkg.in/DataDog/dd-trace-go.v1/profiler"' forwarder/cmd/forwarder/forwarder.go; then
    echo "✅ Profiler package is imported"
else
    echo "❌ Profiler package is NOT imported"
fi

echo ""
echo "2. Checking tracer import in forwarder.go..."
echo "---------------------------------------------"
if grep -q '"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"' forwarder/cmd/forwarder/forwarder.go; then
    echo "✅ Tracer package is imported"
else
    echo "❌ Tracer package is NOT imported"
fi

echo ""
echo "3. Checking tracer initialization..."
echo "-------------------------------------"
if grep -q "tracer.Start(" forwarder/cmd/forwarder/forwarder.go; then
    echo "✅ Tracer initialization found"
    grep -A 4 "tracer.Start(" forwarder/cmd/forwarder/forwarder.go | head -5
else
    echo "❌ Tracer initialization NOT found"
fi

echo ""
echo "4. Checking profiler initialization..."
echo "---------------------------------------"
if grep -q "profiler.Start(" forwarder/cmd/forwarder/forwarder.go; then
    echo "✅ Profiler initialization found"
    grep -A 10 "profiler.Start(" forwarder/cmd/forwarder/forwarder.go | head -11
else
    echo "❌ Profiler initialization NOT found"
fi

echo ""
echo "5. Checking profile types enabled..."
echo "-------------------------------------"
PROFILE_TYPES=("CPUProfile" "HeapProfile" "BlockProfile" "MutexProfile" "GoroutineProfile")
for profile_type in "${PROFILE_TYPES[@]}"; do
    if grep -q "profiler.$profile_type" forwarder/cmd/forwarder/forwarder.go; then
        echo "✅ $profile_type is enabled"
    else
        echo "❌ $profile_type is NOT enabled"
    fi
done

echo ""
echo "6. Checking APM environment check..."
echo "-------------------------------------"
if grep -q "if environment.APMEnabled()" forwarder/cmd/forwarder/forwarder.go; then
    echo "✅ APM enablement check found"
else
    echo "❌ APM enablement check NOT found"
fi

echo ""
echo "7. Testing build..."
echo "-------------------"
cd forwarder
if go build -o /tmp/test-forwarder-profiling-2 cmd/forwarder/forwarder.go 2>/dev/null; then
    echo "✅ Forwarder builds successfully with profiling"
    rm -f /tmp/test-forwarder-profiling-2
else
    echo "❌ Build failed"
fi
cd ..

echo ""
echo "8. Checking environment variables..."
echo "-------------------------------------"
echo "DD_APM_ENABLED=${DD_APM_ENABLED:-not set}"
echo "DD_ENV=${DD_ENV:-not set}"
echo "DD_SERVICE=${DD_SERVICE:-not set}"
echo "DD_VERSION=${DD_VERSION:-not set}"

echo ""
echo "========================================"
echo "Summary"
echo "========================================"
echo ""
echo "When DD_APM_ENABLED=true, the forwarder will:"
echo "1. Initialize the Datadog APM tracer"
echo "2. Start the Datadog profiler"
echo "3. Collect the following profiles:"
echo "   - CPU usage profile"
echo "   - Heap memory profile"
echo "   - Block contention profile"
echo "   - Mutex contention profile"
echo "   - Goroutine profile"
echo "4. Send all profiling data to Datadog"
echo ""
echo "To enable profiling in production:"
echo "1. Set DD_APM_ENABLED=true"
echo "2. Ensure DD_API_KEY is set"
echo "3. Set DD_ENV, DD_SERVICE, DD_VERSION appropriately"
echo "4. Deploy and monitor in Datadog APM & Profiling dashboards"
