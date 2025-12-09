# DocuStitch

DocuStitch is a local document stitching and summarization application designed for processing Code of Federal Regulations (CFR) documents. The application provides a web-based user interface for analyzing and summarizing long documents.

## Overview

This application consists of two main components:

- A FastAPI backend server that handles document processing and API requests
- A React frontend application that provides the user interface

## Prerequisites

Before running the application, ensure you have the following installed on your system:

- Python 3.8 or higher
- Node.js 16 or higher
- npm (comes with Node.js)

## Installation

### Backend Setup

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

### Frontend Setup

1. Navigate to the frontend directory:

```bash
cd frontend
```

2. Install Node.js dependencies:

```bash
npm install
```

3. Return to the project root directory:

```bash
cd ..
```

## Running the Application

### Quick Start

The easiest way to start the application is using the provided startup script:

```bash
./start-ui.sh
```

This script will automatically:
- Start the backend server on port 8000
- Start the frontend development server on port 5173
- Check for and install any missing dependencies

### Manual Start

If you prefer to start the servers manually:

#### Start the Backend

From the project root directory:

```bash
python -m uvicorn api.api:app --reload --port 8000
```

#### Start the Frontend

In a separate terminal, navigate to the frontend directory:

```bash
cd frontend
npm run dev
```

## Accessing the Application

Once both servers are running, you can access:

- Frontend Interface: http://localhost:5173
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Stopping the Application

If you started the application using the startup script, press Ctrl+C in the terminal to stop both servers.

If you started the servers manually, press Ctrl+C in each terminal window where the servers are running.

## Troubleshooting

### Port Already in Use

If you see an error message indicating that a port is already in use, you can find and kill the process using that port:

For the backend (port 8000):
```bash
lsof -ti:8000 | xargs kill -9
```

For the frontend (port 5173):
```bash
lsof -ti:5173 | xargs kill -9
```

Then restart the application.

### Dependencies Not Found

If you encounter errors about missing dependencies:

For Python dependencies:
```bash
pip install -r requirements.txt
```

For frontend dependencies:
```bash
cd frontend
npm install
```

## Project Structure

- `api/` - Backend API code
- `frontend/` - React frontend application
- `docustitch/` - Core document processing modules
- `pipeline/` - Document processing pipeline
- `data/` - Data storage directory
- `artifacts/` - Generated artifacts and outputs
- `conf/` - Configuration files

## Additional Information

For API details and available endpoints, visit the API documentation at http://localhost:8000/docs when the backend server is running.
