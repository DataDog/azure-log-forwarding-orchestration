#!/usr/bin/env python3
"""
Verify that APM environment variables are properly propagated from control plane to forwarders
"""

import sys
import re


def verify_constants_defined():
    """Check that APM constants are defined in cache/env.py"""
    print("Checking control_plane/cache/env.py for APM constants...")

    with open("control_plane/cache/env.py", "r") as f:
        content = f.read()

    constants = ["DD_APM_ENABLED_SETTING", "DD_ENV_SETTING", "DD_SERVICE_SETTING", "DD_VERSION_SETTING"]

    missing = []
    for const in constants:
        if f'{const} = "' in content:
            print(f"  ✅ {const} is defined")
        else:
            print(f"  ❌ {const} is NOT defined")
            missing.append(const)

    return len(missing) == 0


def verify_imports():
    """Check that APM constants are imported in log_forwarder_client.py"""
    print("\nChecking control_plane/tasks/client/log_forwarder_client.py imports...")

    with open("control_plane/tasks/client/log_forwarder_client.py", "r") as f:
        content = f.read()

    # Check for environ import
    if "from os import environ" in content:
        print("  ✅ environ is imported from os")
    else:
        print("  ❌ environ is NOT imported from os")
        return False

    # Check for APM constant imports
    constants = ["DD_APM_ENABLED_SETTING", "DD_ENV_SETTING", "DD_SERVICE_SETTING", "DD_VERSION_SETTING"]

    import_section = re.search(r"from cache\.env import \((.*?)\)", content, re.DOTALL)
    if not import_section:
        print("  ❌ Could not find cache.env import section")
        return False

    imports = import_section.group(1)
    missing = []
    for const in constants:
        if const in imports:
            print(f"  ✅ {const} is imported")
        else:
            print(f"  ❌ {const} is NOT imported")
            missing.append(const)

    return len(missing) == 0


def verify_env_vars_passed():
    """Check that APM env vars are passed in generate_forwarder_settings"""
    print("\nChecking generate_forwarder_settings in log_forwarder_client.py...")

    with open("control_plane/tasks/client/log_forwarder_client.py", "r") as f:
        content = f.read()

    # Find the generate_forwarder_settings method
    method_match = re.search(r"def generate_forwarder_settings.*?(?=\n    def|\n\n|\Z)", content, re.DOTALL)
    if not method_match:
        print("  ❌ Could not find generate_forwarder_settings method")
        return False

    method_content = method_match.group(0)

    env_vars = [
        ("DD_APM_ENABLED_SETTING", "environ.get(DD_APM_ENABLED_SETTING"),
        ("DD_ENV_SETTING", "environ.get(DD_ENV_SETTING"),
        ("DD_SERVICE_SETTING", "environ.get(DD_SERVICE_SETTING"),
        ("DD_VERSION_SETTING", "environ.get(DD_VERSION_SETTING"),
    ]

    missing = []
    for var_name, var_pattern in env_vars:
        if var_pattern in method_content:
            print(f"  ✅ {var_name} is passed to forwarder")
        else:
            print(f"  ❌ {var_name} is NOT passed to forwarder")
            missing.append(var_name)

    return len(missing) == 0


def verify_forwarder_receives():
    """Check that forwarder has constants to receive APM env vars"""
    print("\nChecking forwarder/internal/environment/variables.go...")

    with open("forwarder/internal/environment/variables.go", "r") as f:
        content = f.read()

    constants = [
        ("ApmEnabled", "DD_APM_ENABLED"),
        ("DdEnv", "DD_ENV"),
        ("DdService", "DD_SERVICE"),
        ("DdVersion", "DD_VERSION"),
    ]

    missing = []
    for pattern, name in constants:
        if pattern in content:
            print(f"  ✅ {name} constant is defined in forwarder")
        else:
            print(f"  ❌ {name} constant is NOT defined in forwarder")
            missing.append(name)

    return len(missing) == 0


def main():
    print("=" * 60)
    print("APM Variable Propagation Verification")
    print("=" * 60)

    all_good = True

    if not verify_constants_defined():
        all_good = False

    if not verify_imports():
        all_good = False

    if not verify_env_vars_passed():
        all_good = False

    if not verify_forwarder_receives():
        all_good = False

    print("\n" + "=" * 60)
    if all_good:
        print("✅ SUCCESS: APM variables are properly configured for propagation")
        print("\nWhen the control plane creates new forwarders, they will receive:")
        print("  - DD_APM_ENABLED (from environment or default 'false')")
        print("  - DD_ENV (from environment or default 'production')")
        print("  - DD_SERVICE (from environment or default 'azure-log-forwarder')")
        print("  - DD_VERSION (from environment or default 'latest')")
        return 0
    else:
        print("❌ FAILURE: Some APM variables are not properly configured")
        print("\nPlease fix the issues above to ensure APM works for auto-scaled forwarders")
        return 1


if __name__ == "__main__":
    sys.exit(main())
