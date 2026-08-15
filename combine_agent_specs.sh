find helios/ -type f -name "*.md" -print0 | while IFS= read -r -d $'\0' file; do
    echo ""
    echo "==================================="
    echo "Agent Specification: $file"
    echo "==================================="
    echo ""
    cat "$file"
    echo "" 
done > combined_output.md

