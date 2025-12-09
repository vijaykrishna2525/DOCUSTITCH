import React, { useState, useEffect } from 'react';
import { Download, ChevronDown, ChevronUp } from 'lucide-react';

const ResultDisplay = ({ result }) => {
  const [showSections, setShowSections] = useState(false);
  const [sections, setSections] = useState([]);

  useEffect(() => {
    // Load sections from the upload directory
    if (result && result.upload_id) {
      fetchSections(result.upload_id);
    }
  }, [result]);

  const fetchSections = async (uploadId) => {
    try {
      // Try to fetch sections from the sections endpoint (we'll need to add this)
      // For now, we'll use the sections from result if available
      if (result.sections && result.sections.length > 0) {
        setSections(result.sections);
      }
    } catch (error) {
      console.error('Failed to fetch sections:', error);
    }
  };

  const downloadSummary = () => {
    if (!result || !result.summary) return;
    const blob = new Blob([result.summary], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${result.filename}_summary_3000.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (!result) return null;

  const { filename, doc_type, num_sections, summary, metrics, processing_time, upload_id } = result;

  return (
    <div className="w-full max-w-6xl mx-auto mt-8">
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-xl font-bold text-gray-900">{filename}</h2>
            <p className="text-sm text-gray-500">
              Type: {doc_type.toUpperCase()} | Token Budget: 3000 | Sections: {num_sections}
              {processing_time && ` | Processing time: ${processing_time.toFixed(1)}s`}
            </p>
          </div>
          {summary && (
            <button
              onClick={downloadSummary}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              <Download className="w-4 h-4" />
              Download
            </button>
          )}
        </div>

        {/* Metrics */}
        {metrics && (
          <div className="mb-6 grid grid-cols-2 md:grid-cols-3 gap-4">
            <div className="bg-blue-50 rounded-lg p-3">
              <p className="text-xs font-medium text-blue-600 uppercase">Coverage</p>
              <p className="text-2xl font-bold text-blue-900">{metrics.coverage?.toFixed(1) || 0}%</p>
            </div>
            <div className="bg-green-50 rounded-lg p-3">
              <p className="text-xs font-medium text-green-600 uppercase">ROUGE-L</p>
              <p className="text-2xl font-bold text-green-900">{metrics.rougeL?.toFixed(3) || 0}</p>
            </div>
            <div className="bg-purple-50 rounded-lg p-3">
              <p className="text-xs font-medium text-purple-600 uppercase">Embed Sim</p>
              <p className="text-2xl font-bold text-purple-900">{metrics.embed_sim?.toFixed(3) || 0}</p>
            </div>
            <div className="bg-yellow-50 rounded-lg p-3">
              <p className="text-xs font-medium text-yellow-600 uppercase">Redundancy</p>
              <p className="text-2xl font-bold text-yellow-900">{metrics.redundancy?.toFixed(3) || 0}</p>
            </div>
            {metrics.cue_density !== undefined && metrics.cue_density > 0 && (
              <div className="bg-indigo-50 rounded-lg p-3">
                <p className="text-xs font-medium text-indigo-600 uppercase">Cue Density</p>
                <p className="text-2xl font-bold text-indigo-900">{metrics.cue_density?.toFixed(2)}</p>
              </div>
            )}
          </div>
        )}

        {/* Summary Text */}
        {summary ? (
          <div className="border-t border-gray-200 pt-4">
            <h3 className="text-lg font-semibold mb-3">Summary</h3>
            <div className="bg-gray-50 rounded-lg p-4 max-h-96 overflow-y-auto">
              <pre className="whitespace-pre-wrap text-sm text-gray-800 font-mono leading-relaxed">
                {summary}
              </pre>
            </div>
          </div>
        ) : (
          <div className="border-t border-gray-200 pt-4">
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <p className="text-blue-800">
                ℹ️ <strong>Summary is being generated...</strong>
              </p>
            </div>
          </div>
        )}

        {/* Document Sections */}
        <div className="border-t border-gray-200 pt-4 mt-4">
          <button
            onClick={() => setShowSections(!showSections)}
            className="flex items-center gap-2 text-lg font-semibold text-gray-900 hover:text-gray-700 w-full"
          >
            <span>Document Sections ({num_sections})</span>
            {showSections ? (
              <ChevronUp className="w-5 h-5" />
            ) : (
              <ChevronDown className="w-5 h-5" />
            )}
          </button>

          {showSections && (
            <div className="mt-4 space-y-2 max-h-96 overflow-y-auto">
              {sections && sections.length > 0 ? (
                sections.map((section, index) => (
                  <div
                    key={index}
                    className="border border-gray-200 rounded-lg p-3 hover:bg-gray-50 transition-colors"
                  >
                    <div className="font-medium text-gray-900 text-sm mb-1">
                      {section.sec_id || `Section ${index + 1}`}
                    </div>
                    {section.heading && (
                      <div className="text-xs text-gray-600 mb-1 font-medium">
                        {section.heading}
                      </div>
                    )}
                    {section.text && (
                      <div className="text-xs text-gray-500 line-clamp-2">
                        {section.text.substring(0, 150)}...
                      </div>
                    )}
                  </div>
                ))
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <p>Document sections will appear here after processing</p>
                  <p className="text-sm mt-2">{num_sections} sections parsed</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ResultDisplay;
