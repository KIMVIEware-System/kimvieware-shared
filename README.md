# KIMVIware Shared Library

This package contains the shared libraries and data models for the KIMVIware platform. It provides common functionality for all microservices, including:

*   **MicroserviceBase**: A base class for creating RabbitMQ-based microservices.
*   **Data Models**: Pydantic models for jobs, SUTs, trajectories, etc.
*   **Utilities**: Common utilities for interacting with MongoDB, Redis, and other services.

## Installation

This package is intended to be installed in editable mode from the root of each service that uses it. This ensures that all services are using the same version of the shared code.

To install the library, navigate to the `kimvieware-shared` directory and run:

```bash
pip install -e .
```

This will install the package in editable mode, so any changes to the shared library will be immediately available to the services that use it.

## Dependencies

The shared library has the following dependencies:

*   `pika`: RabbitMQ client
*   `pymongo`: MongoDB client
*   `redis`: Redis client
*   `pydantic`: Data validation and settings management
*   `prometheus-client`: Prometheus instrumentation client

These dependencies are automatically installed when you install the `kimvieware-shared` package.
