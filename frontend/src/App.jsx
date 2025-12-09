import React, { useState } from 'react';
import FileUpload from './components/FileUpload';
import ResultDisplay from './components/ResultDisplay';
import Loading from './components/Loading';
import PrecomputedBrowser from './components/PrecomputedBrowser';
import { uploadFile, uploadFromURL, processDocument, pollUntilComplete } from './services/api';
import { FileUp, Library } from 'lucide-react';
import './index.css';

function App() {
  const [activeTab, setActiveTab] = useState('upload'); // 'upload' or 'browse'
  const [isProcessing, setIsProcessing] = useState(false);
  const [status, setStatus] = useState(null);
  const [progress, setProgress] = useState(0);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleUpload = async (uploadData) => {
    try {
      setIsProcessing(true);
      setError(null);
      setResult(null);
      setProgress(0);

      let uploadResponse;

      // Handle file upload or URL upload
      if (uploadData.type === 'file') {
        setStatus({ message: 'Uploading file...', status: 'uploading' });
        uploadResponse = await uploadFile(uploadData.file);
      } else if (uploadData.type === 'url') {
        setStatus({ message: 'Downloading from URL...', status: 'downloading' });
        uploadResponse = await uploadFromURL(uploadData.url, uploadData.docType);
      }

      const uploadId = uploadResponse.upload_id;

      // Start processing
      setStatus({ message: 'Starting document processing...', status: 'starting' });
      await processDocument(uploadId);

      // Poll for completion
      const finalResult = await pollUntilComplete(uploadId, (statusUpdate) => {
        setStatus(statusUpdate);
        setProgress(statusUpdate.progress || 0);
      });

      // Display results
      setResult(finalResult);
      setStatus({ message: 'Processing completed!', status: 'completed' });
      setProgress(100);
    } catch (err) {
      console.error('Processing error:', err);
      setError(err.message || 'An error occurred during processing');
      setStatus({ message: 'Processing failed', status: 'failed' });
    } finally {
      setIsProcessing(false);
    }
  };

  const handleReset = () => {
    setResult(null);
    setError(null);
    setStatus(null);
    setProgress(0);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">DOCUSTITCH</h1>
              <p className="text-gray-600 mt-1">Document Parsing & Summarization</p>
            </div>
            {result && activeTab === 'upload' && (
              <button
                onClick={handleReset}
                className="px-6 py-2 bg-gray-600 text-white rounded-lg font-medium hover:bg-gray-700 transition-colors"
              >
                Process Another Document
              </button>
            )}
          </div>
        </div>

        {/* Navigation Tabs */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex gap-4 border-b border-gray-200">
            <button
              onClick={() => setActiveTab('upload')}
              className={`flex items-center gap-2 px-4 py-3 font-medium transition-colors border-b-2 ${
                activeTab === 'upload'
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <FileUp className="w-5 h-5" />
              Upload & Process
            </button>
            <button
              onClick={() => setActiveTab('browse')}
              className={`flex items-center gap-2 px-4 py-3 font-medium transition-colors border-b-2 ${
                activeTab === 'browse'
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <Library className="w-5 h-5" />
              Browse Pre-computed
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {activeTab === 'upload' ? (
          <>
            {/* Error Message */}
            {error && (
              <div className="max-w-4xl mx-auto mb-8">
                <div className="bg-red-50 border border-red-200 rounded-xl p-6">
                  <div className="flex items-start">
                    <div className="text-2xl mr-3">❌</div>
                    <div className="flex-1">
                      <h3 className="text-lg font-semibold text-red-900 mb-2">Error</h3>
                      <p className="text-red-800">{error}</p>
                      <button
                        onClick={handleReset}
                        className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg font-medium hover:bg-red-700 transition-colors"
                      >
                        Try Again
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Upload Section or Results */}
            {!result && !isProcessing && !error && (
              <div>
                <div className="text-center mb-8">
                  <h2 className="text-2xl font-bold text-gray-800 mb-2">
                    Upload a Document to Get Started
                  </h2>
                  <p className="text-gray-600">
                    Upload a PDF or XML file, or provide a URL to a document
                  </p>
                </div>
                <FileUpload onUpload={handleUpload} isProcessing={isProcessing} />
              </div>
            )}

            {/* Loading State */}
            {isProcessing && <Loading status={status} progress={progress} />}

            {/* Results */}
            {result && !isProcessing && <ResultDisplay result={result} />}
          </>
        ) : (
          /* Browse Pre-computed Tab */
          <PrecomputedBrowser />
        )}
      </main>

      {/* Footer */}
      <footer className="mt-16 py-8 border-t border-gray-200 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center text-gray-600">
          <p>DOCUSTITCH</p>
        </div>
      </footer>
    </div>
  );
}

export default App;
