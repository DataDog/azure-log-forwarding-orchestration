#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Find repo root - try multiple methods
find_repo_root() {
    # Method 1: Try BASH_SOURCE
    if [[ -n "${BASH_SOURCE[0]}" ]]; then
        local script_dir
        script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"
        if [[ -n "$script_dir" && -d "$script_dir/../forwarder" ]]; then
            echo "$(cd "$script_dir/.." && pwd)"
            return 0
        fi
    fi

    # Method 2: Try git root
    if command -v git &>/dev/null; then
        local git_root
        git_root="$(git rev-parse --show-toplevel 2>/dev/null)"
        if [[ -n "$git_root" && -d "$git_root/forwarder" ]]; then
            echo "$git_root"
            return 0
        fi
    fi

    # Method 3: Search upward from current directory
    local dir="$PWD"
    while [[ "$dir" != "/" ]]; do
        if [[ -d "$dir/forwarder" && -d "$dir/control_plane" ]]; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done

    # Method 4: Check common locations
    for candidate in \
        "$HOME/go/src/github.com/DataDog/azure-log-forwarding-orchestration" \
        "$HOME/dd/azure-log-forwarding-orchestration" \
        "/workspace"; do
        if [[ -d "$candidate/forwarder" && -d "$candidate/control_plane" ]]; then
            echo "$candidate"
            return 0
        fi
    done

    return 1
}

REPO_ROOT="$(find_repo_root)"
if [[ -z "$REPO_ROOT" ]]; then
    echo "ERROR: Could not find repository root. Please run from within the repo."
    exit 1
fi

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if command -v "$1" &> /dev/null; then
        return 0
    else
        return 1
    fi
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin*)    echo "macos" ;;
        Linux*)     echo "linux" ;;
        *)          echo "unknown" ;;
    esac
}

OS=$(detect_os)

# ============================================================================
# Go Installation
# ============================================================================
install_go() {
    local GO_VERSION
    GO_VERSION=$(grep '^go ' "$REPO_ROOT/forwarder/go.mod" | awk '{print $2}')
    if [[ -z "$GO_VERSION" ]]; then
        GO_VERSION="1.25.3"
    fi

    if check_command go; then
        local current_version
        current_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1)
        log_info "Go is already installed: $current_version"

        # Check if version is compatible (1.25+)
        if [[ "$current_version" =~ go1\.(2[5-9]|[3-9][0-9]) ]]; then
            log_info "Go version is compatible"
            return 0
        else
            log_warn "Go version may not be compatible. Required: go$GO_VERSION or later"
        fi
    fi

    log_info "Installing Go $GO_VERSION..."

    if [[ "$OS" == "macos" ]]; then
        if check_command brew; then
            brew install go || brew upgrade go
        else
            log_error "Homebrew not found. Please install Go manually from https://go.dev/dl/"
            return 1
        fi
    elif [[ "$OS" == "linux" ]]; then
        local ARCH
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64)  ARCH="amd64" ;;
            aarch64) ARCH="arm64" ;;
            armv*)   ARCH="armv6l" ;;
        esac

        local GO_TAR="go${GO_VERSION}.linux-${ARCH}.tar.gz"
        local GO_URL="https://go.dev/dl/${GO_TAR}"

        log_info "Downloading Go from $GO_URL..."
        curl -LO "$GO_URL"
        sudo rm -rf /usr/local/go
        sudo tar -C /usr/local -xzf "$GO_TAR"
        rm "$GO_TAR"

        # Add to PATH if not already there
        if ! grep -q '/usr/local/go/bin' ~/.bashrc 2>/dev/null; then
            echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
        fi
        export PATH=$PATH:/usr/local/go/bin
    fi

    log_info "Go installed successfully: $(go version)"
}

# ============================================================================
# Python Installation
# ============================================================================
install_python() {
    local PYTHON_VERSION="3.11"

    if check_command python3; then
        local current_version
        current_version=$(python3 --version | grep -oE '[0-9]+\.[0-9]+')
        log_info "Python is already installed: $current_version"

        # Check if version is 3.11+
        if [[ "$current_version" == "3.11" ]] || [[ "$current_version" == "3.12" ]] || [[ "$current_version" == "3.13" ]]; then
            log_info "Python version is compatible"
            return 0
        else
            log_warn "Python version may not be compatible. Required: Python 3.11+"
        fi
    fi

    log_info "Installing Python $PYTHON_VERSION..."

    if [[ "$OS" == "macos" ]]; then
        if check_command brew; then
            brew install python@3.11 || true
        else
            log_error "Homebrew not found. Please install Python manually."
            return 1
        fi
    elif [[ "$OS" == "linux" ]]; then
        if check_command apt-get; then
            sudo apt-get update
            sudo apt-get install -y python3.11 python3.11-venv python3-pip
        elif check_command yum; then
            sudo yum install -y python3.11 python3.11-pip
        elif check_command dnf; then
            sudo dnf install -y python3.11 python3.11-pip
        else
            log_error "No supported package manager found. Please install Python manually."
            return 1
        fi
    fi

    log_info "Python installed successfully: $(python3 --version)"
}

# ============================================================================
# Forwarder Dependencies (Go)
# ============================================================================
setup_forwarder() {
    log_info "Setting up Forwarder (Go) dependencies..."

    cd "$REPO_ROOT/forwarder"

    # Download Go module dependencies
    log_info "Downloading Go modules..."
    go mod download

    # Verify modules
    log_info "Verifying Go modules..."
    go mod verify

    # Install test coverage tools (optional but useful)
    if ! check_command gocover-cobertura; then
        log_info "Installing gocover-cobertura for coverage reports..."
        go install github.com/boumenot/gocover-cobertura@latest || log_warn "Failed to install gocover-cobertura (optional)"
    fi

    log_info "Forwarder dependencies installed successfully"
    cd "$REPO_ROOT"
}

# ============================================================================
# Control Plane Dependencies (Python)
# ============================================================================
setup_control_plane() {
    log_info "Setting up Control Plane (Python) dependencies..."

    cd "$REPO_ROOT"

    # Create virtual environment if it doesn't exist
    local VENV_DIR="$REPO_ROOT/.venv"
    if [[ ! -d "$VENV_DIR" ]]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    # Activate virtual environment
    log_info "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"

    # Upgrade pip
    log_info "Upgrading pip..."
    pip install --upgrade pip

    # Install control_plane with all extras including dev dependencies
    log_info "Installing control_plane package with dev dependencies..."
    pip install -e './control_plane[dev]'

    # Install additional coverage tools
    log_info "Installing coverage tools..."
    pip install coverage pycobertura

    log_info "Control Plane dependencies installed successfully"
    log_info "Virtual environment located at: $VENV_DIR"
    log_info "Activate with: source $VENV_DIR/bin/activate"

    cd "$REPO_ROOT"
}

# ============================================================================
# Loggy Dependencies (Python - Azure Functions)
# ============================================================================
setup_loggy() {
    log_info "Setting up Loggy (Azure Functions) dependencies..."

    cd "$REPO_ROOT"

    # Use existing virtual environment or create one
    local VENV_DIR="$REPO_ROOT/.venv"
    if [[ ! -d "$VENV_DIR" ]]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    source "$VENV_DIR/bin/activate"

    # Install loggy dependencies
    if [[ -f "$REPO_ROOT/loggy/requirements.txt" ]]; then
        log_info "Installing Loggy requirements..."
        pip install -r "$REPO_ROOT/loggy/requirements.txt"
    fi

    log_info "Loggy dependencies installed successfully"
    cd "$REPO_ROOT"
}

# ============================================================================
# Pre-commit Hooks (Optional)
# ============================================================================
setup_precommit() {
    log_info "Setting up pre-commit hooks..."

    source "$REPO_ROOT/.venv/bin/activate" 2>/dev/null || true

    if ! check_command pre-commit; then
        pip install pre-commit
    fi

    cd "$REPO_ROOT"
    pre-commit install

    log_info "Pre-commit hooks installed"
}

# ============================================================================
# Verification
# ============================================================================
verify_setup() {
    log_info "Verifying setup..."

    local errors=0

    # Check Go
    if check_command go; then
        log_info "Go: $(go version)"
    else
        log_error "Go is not available"
        ((errors++))
    fi

    # Check Python
    if check_command python3; then
        log_info "Python: $(python3 --version)"
    else
        log_error "Python3 is not available"
        ((errors++))
    fi

    # Check Go modules
    if [[ -f "$REPO_ROOT/forwarder/go.sum" ]]; then
        log_info "Forwarder Go modules: OK"
    else
        log_warn "Forwarder Go modules may not be fully downloaded"
    fi

    # Check Python virtual environment
    if [[ -d "$REPO_ROOT/.venv" ]]; then
        log_info "Python virtual environment: OK"
    else
        log_warn "Python virtual environment not found"
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "Setup verification failed with $errors error(s)"
        return 1
    fi

    log_info "Setup verification completed successfully"
}

# ============================================================================
# Run Tests
# ============================================================================
run_forwarder_tests() {
    log_info "Running Forwarder tests..."
    cd "$REPO_ROOT/forwarder"
    go test -race -v ./...
    cd "$REPO_ROOT"
}

run_control_plane_tests() {
    log_info "Running Control Plane tests..."
    source "$REPO_ROOT/.venv/bin/activate"
    cd "$REPO_ROOT"
    pytest ./control_plane -v
}

# ============================================================================
# Main
# ============================================================================
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Install all dependencies for the azure-log-forwarding-orchestration repository.

OPTIONS:
    --all           Install everything (default)
    --forwarder     Install only Forwarder (Go) dependencies
    --control-plane Install only Control Plane (Python) dependencies
    --loggy         Install only Loggy (Azure Functions) dependencies
    --precommit     Install pre-commit hooks
    --verify        Verify the setup
    --test          Run all unit tests after setup
    --test-forwarder Run Forwarder unit tests
    --test-control-plane Run Control Plane unit tests
    -h, --help      Show this help message

EXAMPLES:
    $(basename "$0")                    # Install all dependencies
    $(basename "$0") --forwarder        # Install only Go dependencies
    $(basename "$0") --control-plane    # Install only Python dependencies
    $(basename "$0") --all --test       # Install all and run tests
EOF
}

main() {
    local install_all=true
    local install_forwarder=false
    local install_control_plane=false
    local install_loggy=false
    local install_precommit=false
    local run_verify=false
    local run_tests=false
    local run_forwarder_test=false
    local run_control_plane_test=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all)
                install_all=true
                shift
                ;;
            --forwarder)
                install_all=false
                install_forwarder=true
                shift
                ;;
            --control-plane)
                install_all=false
                install_control_plane=true
                shift
                ;;
            --loggy)
                install_all=false
                install_loggy=true
                shift
                ;;
            --precommit)
                install_precommit=true
                shift
                ;;
            --verify)
                run_verify=true
                shift
                ;;
            --test)
                run_tests=true
                shift
                ;;
            --test-forwarder)
                run_forwarder_test=true
                shift
                ;;
            --test-control-plane)
                run_control_plane_test=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    log_info "============================================"
    log_info "Azure Log Forwarding Orchestration - Setup"
    log_info "============================================"
    log_info "Repository: $REPO_ROOT"
    log_info "OS: $OS"
    log_info ""

    # Install language runtimes
    if [[ "$install_all" == true ]] || [[ "$install_forwarder" == true ]]; then
        install_go
    fi

    if [[ "$install_all" == true ]] || [[ "$install_control_plane" == true ]] || [[ "$install_loggy" == true ]]; then
        install_python
    fi

    # Install component dependencies
    if [[ "$install_all" == true ]] || [[ "$install_forwarder" == true ]]; then
        setup_forwarder
    fi

    if [[ "$install_all" == true ]] || [[ "$install_control_plane" == true ]]; then
        setup_control_plane
    fi

    if [[ "$install_all" == true ]] || [[ "$install_loggy" == true ]]; then
        setup_loggy
    fi

    # Optional: pre-commit hooks
    if [[ "$install_precommit" == true ]]; then
        setup_precommit
    fi

    # Verification
    if [[ "$run_verify" == true ]] || [[ "$install_all" == true ]]; then
        verify_setup
    fi

    # Run tests if requested
    if [[ "$run_tests" == true ]]; then
        run_forwarder_tests
        run_control_plane_tests
    fi

    if [[ "$run_forwarder_test" == true ]]; then
        run_forwarder_tests
    fi

    if [[ "$run_control_plane_test" == true ]]; then
        run_control_plane_tests
    fi

    log_info ""
    log_info "============================================"
    log_info "Setup completed successfully!"
    log_info "============================================"
    log_info ""
    log_info "To run Forwarder tests:"
    log_info "  cd forwarder && go test -race -v ./..."
    log_info ""
    log_info "To run Control Plane tests:"
    log_info "  source .venv/bin/activate"
    log_info "  pytest ./control_plane -v"
    log_info ""
}

main "$@"
