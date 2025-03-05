# FlashFS Documentation

This directory contains the comprehensive documentation for FlashFS, a high-performance file system snapshot and diff tool.

## Documentation Files

| File | Description |
|------|-------------|
| [index.md](index.md) | Overview of FlashFS and its components |
| [cli.md](cli.md) | Command-line interface documentation |
| [walker.md](walker.md) | File system traversal component documentation |
| [serializer.md](serializer.md) | Serialization component documentation |
| [schema.md](schema.md) | Data schema definitions and usage |
| [storage.md](storage.md) | Storage management documentation |
| [diff-computation.md](diff-computation.md) | Diff computation and application |
| [expiry-policy.md](expiry-policy.md) | Snapshot expiry policy documentation |

## Building the Documentation

This documentation is designed to be used with GitHub Pages using a Material theme. To build and view the documentation locally:

1. Install MkDocs with the Material theme:

   ```bash
   pip install mkdocs-material
   ```

2. Run the local development server:

   ```bash
   mkdocs serve
   ```

3. Open your browser to <http://localhost:8000> to view the documentation.

## Documentation Structure

The documentation is organized by component, with each file providing a comprehensive overview of a specific aspect of FlashFS. The documentation includes:

- **Overview**: A high-level explanation of each component's purpose
- **API Reference**: Detailed information about functions and data structures
- **Usage Examples**: Code snippets demonstrating how to use the component
- **Implementation Details**: Insights into the implementation for advanced users

## Contributing to Documentation

When contributing to this documentation:

1. Use clear, concise language
2. Include code examples for key functionality
3. Keep examples up-to-date with the current API
4. Use markdown formatting consistently
5. Test any command examples to ensure they work as expected

## License

This documentation is licensed under the same license as the FlashFS project.
