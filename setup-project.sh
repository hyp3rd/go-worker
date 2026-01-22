#!/usr/bin/env bash
set -e

# ######################################
# Replace #PROJECT placeholder with actual git repository name
# This script should be run after cloning the starter template
# ######################################

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Replace #PROJECT placeholder with the actual Go module name.

OPTIONS:
    -m, --module <name>     Specify the module name explicitly (e.g., github.com/user/repo)
    -h, --help              Show this help message

EXAMPLES:
    $0                                          # Auto-detect from go.mod or git remote
    $0 --module github.com/myuser/myproject     # Use explicit module name

EOF
    exit 0
}

# Parse command line arguments
MODULE_NAME=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--module)
            MODULE_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: Unknown option: $1"
            usage
            ;;
    esac
done

echo "Setting up project..."
echo "====================="
echo ""

# Get module name from go.mod if it exists
get_module_from_gomod() {
    if [[ -f "go.mod" ]]; then
        local module
        module=$(grep -E "^module " go.mod | awk '{print $2}')
        if [[ -n "$module" ]]; then
            echo "$module"
            return 0
        fi
    fi
    return 1
}

# Get the git repository name from remote URL in Go module format
get_repo_name_from_git() {
    local remote_url
    remote_url=$(git remote get-url origin 2>/dev/null || echo "")

    if [[ -z "$remote_url" ]]; then
        echo "Warning: No git remote found. Trying to get repo name from directory..." >&2
        echo "github.com/unknown/$(basename "$(git rev-parse --show-toplevel 2>/dev/null || pwd)")"
    else
        # Extract repo name from URL (handles both SSH and HTTPS)
        # Examples:
        #   git@github.com:user/repo.git -> github.com/user/repo
        #   https://github.com/user/repo.git -> github.com/user/repo
        #   git@gitlab.com:user/repo.git -> gitlab.com/user/repo

        # Remove .git suffix if present
        remote_url="${remote_url%.git}"

        # Handle SSH format (git@host:user/repo)
        if [[ "$remote_url" =~ ^git@([^:]+):(.+)$ ]]; then
            echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
        # Handle HTTPS format (https://host/user/repo)
        elif [[ "$remote_url" =~ ^https?://([^/]+)/(.+)$ ]]; then
            echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
        else
            echo "Error: Could not parse remote URL: $remote_url" >&2
            return 1
        fi
    fi
}

# Determine the module name
determine_module_name() {
    local module=""

    # 1. Use explicit module name if provided
    if [[ -n "$MODULE_NAME" ]]; then
        echo "Using module name from command line: $MODULE_NAME" >&2
        echo "$MODULE_NAME"
        return 0
    fi

    # 2. Try to get from existing go.mod
    if module=$(get_module_from_gomod); then
        echo "Found existing Go module in go.mod: $module" >&2
        echo "$module"
        return 0
    fi

    # 3. Fall back to git remote detection
    echo "No go.mod found, detecting from git remote..." >&2
    if module=$(get_repo_name_from_git); then
        echo "Detected module name from git: $module" >&2
        echo "$module"
        return 0
    fi

    return 1
}

# Get the repository/module name
REPO_NAME=$(determine_module_name)

if [[ -z "$REPO_NAME" ]]; then
    echo "Error: Could not determine module name"
    echo "Please provide it explicitly using --module flag"
    echo "Example: $0 --module github.com/user/repo"
    exit 1
fi

echo ""
echo "Module name: $REPO_NAME"
echo ""

# Initialize go module if it doesn't exist
if [[ ! -f "go.mod" ]]; then
    echo "Initializing Go module..."
    if command -v go &> /dev/null; then
        go mod init "$REPO_NAME"
        echo "  ✓ Created go.mod with module $REPO_NAME"
    else
        echo "  ⚠ Warning: go command not found, skipping go mod init"
        echo "  You'll need to run 'go mod init $REPO_NAME' manually"
    fi
    echo ""
fi

# Files to update
FILES_TO_UPDATE=(
	".pre-commit/gci-hook"
	"Makefile"
	"go.mod"
	"cmd/app/main.go"
	"buf.gen.yaml"
	"api/core/v1/health.proto"
	".project-settings.env"
	".github/workflows/lint.yml"
)

# Check if files exist and replace placeholders
for file in "${FILES_TO_UPDATE[@]}"; do
	if [[ -f "$file" ]]; then
		echo "Updating $file..."

		updated=""
		ensure_backup() {
			if [[ -z "$updated" ]]; then
				cp "$file" "$file.bak"
			fi
		}

		if grep -q "#PROJECT" "$file"; then
			ensure_backup
			sed -i.tmp "s|#PROJECT|$REPO_NAME|g" "$file"
			rm -f "$file.tmp"
			updated="yes"
			echo "  ✓ Replaced #PROJECT with $REPO_NAME"
		fi

		if grep -q "github.com/hyp3rd/starter" "$file"; then
			ensure_backup
			sed -i.tmp "s|github.com/hyp3rd/starter|$REPO_NAME|g" "$file"
			rm -f "$file.tmp"
			updated="yes"
			echo "  ✓ Replaced github.com/hyp3rd/starter with $REPO_NAME"
		fi

		if [[ "$file" == "go.mod" ]] && grep -qE "^module " "$file"; then
			ensure_backup
			sed -i.tmp "s|^module .*|module $REPO_NAME|" "$file"
			rm -f "$file.tmp"
			updated="yes"
			echo "  ✓ Set module to $REPO_NAME"
		fi

		if [[ "$file" == ".project-settings.env" ]] && grep -q "^GCI_PREFIX=" "$file"; then
			ensure_backup
			sed -i.tmp "s|^GCI_PREFIX=.*|GCI_PREFIX=$REPO_NAME|" "$file"
			rm -f "$file.tmp"
			updated="yes"
			echo "  ✓ Set GCI_PREFIX to $REPO_NAME"
		fi

		if [[ -z "$updated" ]]; then
			echo "  ℹ No placeholders found"
		fi
	else
		echo "  ⚠ Warning: $file not found, skipping..."
	fi
done

echo ""
echo "Setup complete!"
echo "Backup files created with .bak extension"
echo ""
echo "To restore backups, run:"
for file in "${FILES_TO_UPDATE[@]}"; do
    if [[ -f "$file.bak" ]]; then
        echo "  mv $file.bak $file"
    fi
done
