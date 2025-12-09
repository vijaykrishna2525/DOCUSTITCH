import React, { useState } from 'react';

const FileUpload = ({ onUpload, isProcessing }) => {
  const [uploadMode, setUploadMode] = useState('file'); // 'file' or 'url'
  const [url, setUrl] = useState('');
  const [docType, setDocType] = useState('xml');
  const [dragActive, setDragActive] = useState(false);

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFileUpload(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      handleFileUpload(e.target.files[0]);
    }
  };

  const handleFileUpload = (file) => {
    // Validate file type
    const fileName = file.name.toLowerCase();
    if (!fileName.endsWith('.pdf') && !fileName.endsWith('.xml')) {
      alert('Please upload a PDF or XML file');
      return;
    }

    onUpload({ type: 'file', file });
  };

  const handleURLUpload = () => {
    if (!url.trim()) {
      alert('Please enter a URL');
      return;
    }

    onUpload({ type: 'url', url, docType });
  };

  return (
    <div className="w-full max-w-4xl mx-auto">
      {/* Upload Mode Selector */}
      <div className="flex gap-4 mb-6">
        <button
          onClick={() => setUploadMode('file')}
          className={`flex-1 py-3 px-6 rounded-lg font-medium transition-all ${
            uploadMode === 'file'
              ? 'bg-blue-600 text-white shadow-lg'
              : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-300'
          }`}
          disabled={isProcessing}
        >
          📄 Upload File
        </button>
        <button
          onClick={() => setUploadMode('url')}
          className={`flex-1 py-3 px-6 rounded-lg font-medium transition-all ${
            uploadMode === 'url'
              ? 'bg-blue-600 text-white shadow-lg'
              : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-300'
          }`}
          disabled={isProcessing}
        >
          🔗 From URL
        </button>
      </div>

      {/* File Upload Mode */}
      {uploadMode === 'file' && (
        <div
          className={`relative border-2 border-dashed rounded-xl p-12 text-center transition-all ${
            dragActive
              ? 'border-blue-500 bg-blue-50'
              : 'border-gray-300 bg-white hover:border-gray-400'
          } ${isProcessing ? 'opacity-50 pointer-events-none' : ''}`}
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
        >
          <input
            type="file"
            id="file-upload"
            accept=".pdf,.xml"
            onChange={handleChange}
            className="hidden"
            disabled={isProcessing}
          />

          <label
            htmlFor="file-upload"
            className="cursor-pointer flex flex-col items-center"
          >
            <div className="text-6xl mb-4">📄</div>
            <p className="text-xl font-semibold text-gray-700 mb-2">
              Drop your file here or click to browse
            </p>
            <p className="text-gray-500">
              Supports PDF and XML files
            </p>
            <button
              type="button"
              className="mt-6 px-6 py-3 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors"
              onClick={() => document.getElementById('file-upload').click()}
              disabled={isProcessing}
            >
              Choose File
            </button>
          </label>
        </div>
      )}

      {/* URL Upload Mode */}
      {uploadMode === 'url' && (
        <div className={`bg-white rounded-xl p-8 shadow-md ${isProcessing ? 'opacity-50 pointer-events-none' : ''}`}>
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Document URL
            </label>
            <input
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://example.com/document.xml"
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
              disabled={isProcessing}
            />
            <p className="mt-2 text-sm text-gray-500">
              Only XML documents are supported for URL upload
            </p>
          </div>

          <button
            onClick={handleURLUpload}
            className="w-full py-3 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors"
            disabled={isProcessing}
          >
            Process Document from URL
          </button>
        </div>
      )}
    </div>
  );
};

export default FileUpload;
