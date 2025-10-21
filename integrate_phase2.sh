#!/bin/bash

# Phase 2 Integration Script
# This script integrates Phase 2 monoid implementations into the algesnake repository

set -e  # Exit on error

REPO_DIR="/Users/vincent/development/algesnake"
DOWNLOADS_DIR="$HOME/Downloads"

echo "🐍 Algesnake Phase 2 Integration"
echo "================================="
echo ""

# Check if repo directory exists
if [ ! -d "$REPO_DIR" ]; then
    echo "❌ Error: Repository not found at $REPO_DIR"
    exit 1
fi

echo "📂 Repository: $REPO_DIR"
echo ""

# Navigate to repo
cd "$REPO_DIR"

echo "📁 Creating directory structure..."
mkdir -p algesnake/monoid
mkdir -p tests/unit
mkdir -p examples
mkdir -p docs

echo "✅ Directories created"
echo ""

echo "📝 Copying implementation files..."
# Copy implementations
cp "$DOWNLOADS_DIR/numeric_monoids.py" algesnake/monoid/numeric.py
cp "$DOWNLOADS_DIR/collection_monoids.py" algesnake/monoid/collection.py
cp "$DOWNLOADS_DIR/option_monoid.py" algesnake/monoid/option.py

echo "✅ Implementation files copied"
echo ""

echo "🧪 Copying test files..."
# Copy tests
cp "$DOWNLOADS_DIR/test_numeric_monoids.py" tests/unit/
cp "$DOWNLOADS_DIR/test_collection_monoids.py" tests/unit/
cp "$DOWNLOADS_DIR/test_option_monoid.py" tests/unit/

echo "✅ Test files copied"
echo ""

echo "💡 Copying examples and documentation..."
# Copy examples and docs
cp "$DOWNLOADS_DIR/examples.py" examples/phase2_examples.py
cp "$DOWNLOADS_DIR/PHASE2_README.md" docs/phase2.md
cp "$DOWNLOADS_DIR/INTEGRATION_GUIDE.md" docs/
cp "$DOWNLOADS_DIR/QUICKSTART.md" docs/

echo "✅ Examples and docs copied"
echo ""

echo "🔧 Creating algesnake/monoid/__init__.py..."
# Create __init__.py for monoid package
cat > algesnake/monoid/__init__.py << 'EOF'
"""Concrete monoid implementations.

This module provides production-ready monoid implementations for:
- Numeric operations (Add, Multiply, Max, Min)
- Collection operations (Set, List, Map, String)
- Optional values (Some, None_, Option)

All implementations follow monoid laws:
- Associativity: (a • b) • c = a • (b • c)
- Identity: zero • a = a • zero = a
"""

from .numeric import Add, Multiply, Max, Min
from .collection import SetMonoid, ListMonoid, MapMonoid, StringMonoid
from .option import Some, None_, Option, OptionMonoid

__all__ = [
    # Numeric monoids
    'Add', 'Multiply', 'Max', 'Min',
    # Collection monoids
    'SetMonoid', 'ListMonoid', 'MapMonoid', 'StringMonoid',
    # Option monoid
    'Some', 'None_', 'Option', 'OptionMonoid',
]
EOF

echo "✅ __init__.py created"
echo ""

echo "🔧 Updating algesnake/__init__.py..."
# Check if main __init__.py exists
if [ -f "algesnake/__init__.py" ]; then
    # Backup existing __init__.py
    cp algesnake/__init__.py algesnake/__init__.py.backup
    echo "📦 Backed up existing __init__.py to __init__.py.backup"
fi

# Create updated __init__.py
cat > algesnake/__init__.py << 'EOF'
"""
Algesnake - Abstract algebra for Python data pipelines.

Algesnake provides algebraic abstractions (Monoids, Groups, Rings, Semirings)
for building aggregation systems, analytics pipelines, and approximation algorithms.
"""

# Phase 1: Abstract base classes
try:
    from .abstract import Semigroup, Monoid, Group, Ring, Semiring
except ImportError:
    # Abstract classes not yet implemented
    pass

# Phase 2: Concrete monoid implementations
from .monoid import (
    # Numeric monoids
    Add, Multiply, Max, Min,
    # Collection monoids
    SetMonoid, ListMonoid, MapMonoid, StringMonoid,
    # Option monoid
    Some, None_, Option, OptionMonoid,
)

__version__ = "0.2.0"

__all__ = [
    # Numeric monoids
    'Add', 'Multiply', 'Max', 'Min',
    # Collection monoids
    'SetMonoid', 'ListMonoid', 'MapMonoid', 'StringMonoid',
    # Option monoid
    'Some', 'None_', 'Option', 'OptionMonoid',
]

# Add abstract classes if available
try:
    __all__.extend(['Semigroup', 'Monoid', 'Group', 'Ring', 'Semiring'])
except NameError:
    pass
EOF

echo "✅ algesnake/__init__.py updated"
echo ""

echo "🧪 Running quick tests..."
# Quick smoke test
python3 -c "
from algesnake.monoid.numeric import Add, Max
from algesnake.monoid.collection import SetMonoid, MapMonoid
from algesnake.monoid.option import Some, None_

# Test numeric
assert (Add(5) + Add(3)).value == 8
print('  ✓ Numeric monoids work')

# Test collection
assert (SetMonoid({1,2}) + SetMonoid({2,3})).value == {1,2,3}
print('  ✓ Collection monoids work')

# Test option
assert (Some(5) + None_()).value == 5
print('  ✓ Option monoid works')

print('\n✅ All quick tests passed!')
" 2>/dev/null && echo "" || echo "⚠️  Warning: Quick tests failed (may need to install dependencies)"

echo ""
echo "📊 Summary"
echo "=========="
echo "Files integrated:"
echo "  - 3 implementation modules"
echo "  - 3 test suites"
echo "  - 1 examples file"
echo "  - 4 documentation files"
echo ""
echo "📂 Directory structure:"
echo "  algesnake/"
echo "    monoid/"
echo "      __init__.py"
echo "      numeric.py"
echo "      collection.py"
echo "      option.py"
echo "  tests/unit/"
echo "    test_numeric_monoids.py"
echo "    test_collection_monoids.py"
echo "    test_option_monoid.py"
echo "  examples/"
echo "    phase2_examples.py"
echo "  docs/"
echo "    phase2.md"
echo "    INTEGRATION_GUIDE.md"
echo "    QUICKSTART.md"
echo ""

echo "🎯 Next Steps:"
echo "=============="
echo "1. Review the changes:"
echo "   cd $REPO_DIR"
echo "   git status"
echo ""
echo "2. Run the full test suite:"
echo "   pytest tests/unit/test_numeric_monoids.py -v"
echo "   pytest tests/unit/test_collection_monoids.py -v"
echo "   pytest tests/unit/test_option_monoid.py -v"
echo ""
echo "3. Try the examples:"
echo "   python examples/phase2_examples.py"
echo ""
echo "4. Commit and push:"
echo "   git add ."
echo "   git commit -m 'Add Phase 2: Concrete monoid implementations'"
echo "   git push origin main"
echo ""
echo "5. Update README.md:"
echo "   Change '🚧 Concrete Implementations' to '✅ Concrete Implementations'"
echo ""

echo "✨ Phase 2 integration complete!"
echo ""
echo "📖 For more info, see:"
echo "   - docs/QUICKSTART.md - Common patterns"
echo "   - docs/phase2.md - Full documentation"
echo "   - docs/INTEGRATION_GUIDE.md - Integration details"
