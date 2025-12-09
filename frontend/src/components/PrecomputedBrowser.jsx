import { useState, useEffect } from 'react';
import { FileText, Download } from 'lucide-react';

export default function PrecomputedBrowser() {
  const [documents, setDocuments] = useState([]);
  const [selectedDoc, setSelectedDoc] = useState(null);
  const [selectedBudget, setSelectedBudget] = useState(null);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchDocuments();
  }, []);

  const fetchDocuments = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/documents');
      const data = await response.json();
      setDocuments(data.documents);
    } catch (error) {
      console.error('Failed to fetch documents:', error);
    }
  };

  const fetchSummary = async (docId, budget) => {
    setLoading(true);
    try {
      const response = await fetch(`http://localhost:8000/api/precomputed/${docId}/${budget}`);
      const data = await response.json();
      setSummary(data);
      setSelectedDoc(docId);
      setSelectedBudget(budget);
    } catch (error) {
      console.error('Failed to fetch summary:', error);
    } finally {
      setLoading(false);
    }
  };

  const downloadSummary = () => {
    if (!summary) return;
    const blob = new Blob([summary.summary], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${summary.doc_id}_summary_${summary.budget}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="max-w-7xl mx-auto p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Pre-computed Summaries</h1>
        <p className="text-gray-600">Browse and view existing document summaries with evaluation metrics</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Document List */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
            <h2 className="text-lg font-semibold mb-4">Available Documents</h2>
            <div className="space-y-2">
              {documents.map((doc) => (
                <div key={doc.doc_id} className="border border-gray-200 rounded-lg p-3">
                  <div className="flex items-start gap-2">
                    <FileText className="w-5 h-5 text-blue-500 mt-0.5" />
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-gray-900 truncate">{doc.doc_id}</p>
                      <p className="text-sm text-gray-500">
                        {doc.available_budgets.length} token budgets
                      </p>
                      <div className="mt-2 flex flex-wrap gap-1">
                        {doc.available_budgets.map((budget) => (
                          <button
                            key={budget}
                            onClick={() => fetchSummary(doc.doc_id, budget)}
                            className={`px-2 py-1 text-xs font-medium rounded ${
                              selectedDoc === doc.doc_id && selectedBudget === budget
                                ? 'bg-blue-600 text-white'
                                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                            }`}
                          >
                            {budget}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Summary Display */}
        <div className="lg:col-span-2">
          {loading ? (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
              <p className="mt-4 text-gray-600">Loading summary...</p>
            </div>
          ) : summary ? (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h2 className="text-xl font-bold text-gray-900">{summary.doc_id}</h2>
                  <p className="text-sm text-gray-500">
                    Token Budget: {summary.budget} | Sections: {summary.num_sections}
                  </p>
                </div>
                <button
                  onClick={downloadSummary}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  <Download className="w-4 h-4" />
                  Download
                </button>
              </div>

              {/* Metrics */}
              {summary.metrics && (
                <div className="mb-6 grid grid-cols-2 md:grid-cols-3 gap-4">
                  <div className="bg-blue-50 rounded-lg p-3">
                    <p className="text-xs font-medium text-blue-600 uppercase">Coverage</p>
                    <p className="text-2xl font-bold text-blue-900">{summary.metrics.coverage.toFixed(1)}%</p>
                  </div>
                  <div className="bg-green-50 rounded-lg p-3">
                    <p className="text-xs font-medium text-green-600 uppercase">ROUGE-L</p>
                    <p className="text-2xl font-bold text-green-900">{summary.metrics.rougeL.toFixed(3)}</p>
                  </div>
                  <div className="bg-purple-50 rounded-lg p-3">
                    <p className="text-xs font-medium text-purple-600 uppercase">Embed Sim</p>
                    <p className="text-2xl font-bold text-purple-900">{summary.metrics.embed_sim.toFixed(3)}</p>
                  </div>
                  <div className="bg-yellow-50 rounded-lg p-3">
                    <p className="text-xs font-medium text-yellow-600 uppercase">Redundancy</p>
                    <p className="text-2xl font-bold text-yellow-900">{summary.metrics.redundancy.toFixed(3)}</p>
                  </div>
                  {summary.metrics.cue_density > 0 && (
                    <div className="bg-indigo-50 rounded-lg p-3">
                      <p className="text-xs font-medium text-indigo-600 uppercase">Cue Density</p>
                      <p className="text-2xl font-bold text-indigo-900">{summary.metrics.cue_density.toFixed(2)}</p>
                    </div>
                  )}
                </div>
              )}

              {/* Summary Text */}
              <div className="border-t border-gray-200 pt-4">
                <h3 className="text-lg font-semibold mb-3">Summary</h3>
                <div className="bg-gray-50 rounded-lg p-4 max-h-96 overflow-y-auto">
                  <pre className="whitespace-pre-wrap text-sm text-gray-800 font-mono leading-relaxed">
                    {summary.summary}
                  </pre>
                </div>
                <p className="text-xs text-gray-500 mt-2">
                  Source: {summary.summary_file}
                </p>
              </div>
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
              <FileText className="w-16 h-16 text-gray-300 mx-auto mb-4" />
              <p className="text-gray-600">Select a document and token budget to view the summary</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
